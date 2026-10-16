"""完整真实 AIS 导出在独立 Kafka 中回放；两版输入、落盘和进度逐条核对。"""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import resource
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from zoneinfo import ZoneInfo

from confluent_kafka import Consumer, Producer, TopicPartition, libversion
from bridge_kafka.cli import init_topic
from bridge_kafka.offline import jsonl_records
from bridge_kafka.settings import ROOT, load_settings
from bridge_kafka.supplement import PREPARED
from tools.common import AIS_CHANNELS, BINARY, write_config
from tools.validation.cluster import local_cluster
from tools.validation.compare import compare_databases


def source_digest():
    digest=hashlib.sha256()
    for p in [*sorted((ROOT/'src/python/bridge_kafka').glob('*.py')),*sorted((ROOT/'src/rust/src').glob('*.rs'))]:
        digest.update(p.read_bytes())
    return digest.hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rounds',type=int,default=4)
    parser.add_argument('--warmup',type=int,default=5000)
    parser.add_argument('--before-root',type=Path,help='本次验证的临时融合前代码/二进制目录；验证后清理')
    args=parser.parse_args()
    if args.rounds<2 or args.warmup<1:
        parser.error('rounds >= 2, warmup >= 1')
    versions=json.loads(subprocess.check_output([str(BINARY),'--versions'],text=True))
    assert versions=={'librdkafka':libversion()[0],'sqlite':sqlite3.sqlite_version}
    before_manifest=None
    languages=('python','rust')
    if args.before_root:
        args.before_root=args.before_root.resolve()
        before_manifest=json.loads((args.before_root/'manifest.json').read_text())
        assert (args.before_root/'manage.py').is_file() and (args.before_root/'rust-before').is_file()
        assert json.loads(subprocess.check_output([str(args.before_root/'rust-before'),'--versions'],text=True))==versions
        languages=('python-before','python','rust-before','rust')
    initial_code=source_digest()
    stamp=datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    folder=ROOT/'reports/performance/continuous-ais'/stamp
    folder.mkdir(parents=True)
    channels=[]
    with local_cluster() as bootstrap,tempfile.TemporaryDirectory(prefix='ct-real-throughput-') as directory:
        work=Path(directory)
        for topic,config_name in AIS_CHANNELS:
            source=PREPARED/(topic+'.jsonl')
            payloads=[raw for _,raw in jsonl_records(source)]
            count=len(payloads)
            if args.warmup>count:
                raise ValueError('warmup 超过该来源的实际行数')
            settings=load_settings(ROOT/config_name)
            init_topic({**settings,'client':{**settings['client'],'bootstrap.servers':bootstrap}})
            producer=Producer({'bootstrap.servers':bootstrap,'enable.idempotence':True,'acks':'all',
                               'delivery.timeout.ms':30000,'linger.ms':5,'batch.size':65536})
            failures=[]
            def delivered(error,_):
                if error:
                    failures.append(str(error))
            for i,raw in enumerate(payloads):
                identifier=str(json.loads(raw)['mmsi'])
                key=identifier.encode('ascii') if len(identifier)==9 and identifier.isascii() and identifier.isdigit() else None
                while True:
                    try:
                        producer.produce(topic,key=key,value=raw,on_delivery=delivered)
                        break
                    except BufferError:
                        producer.poll(.05)
                if i%512==0:
                    producer.poll(0)
            assert producer.flush(45)==0 and not failures
            del producer

            def run(language,label,n):
                db=work/(topic+'-'+label+'-'+language+'.db')
                group=topic+'-'+label+'-'+language
                if language in ('python','python-before'):
                    config=work/(group+'.toml')
                    write_config(config,config_name,bootstrap,group,db)
                    command=[sys.executable,str((args.before_root if language=='python-before' else ROOT)/'manage.py'),'python','--config',str(config),
                             'consume','--max-messages',str(n),'--idle-timeout','30']
                else:
                    command=[str(args.before_root/'rust-before' if language=='rust-before' else BINARY),'--bootstrap-servers',bootstrap,'--topic',topic,'--group',group,
                             '--database',str(db),'--count',str(n),'--idle-timeout','30']
                log_path=folder/(group+'.log')
                before=resource.getrusage(resource.RUSAGE_CHILDREN)
                started=time.perf_counter()
                with log_path.open('wb') as log:
                    result=subprocess.run(command,cwd=ROOT,stdout=log,stderr=log,timeout=180)
                seconds=time.perf_counter()-started
                after=resource.getrusage(resource.RUSAGE_CHILDREN)
                assert result.returncode==0,f'{language}/{topic} failed; see {log_path}'
                active=None
                for line in log_path.read_text().splitlines():
                    if line.startswith('{'):
                        active=json.loads(line).get('active_seconds',active)
                reader=Consumer({'bootstrap.servers':bootstrap,'group.id':group,'enable.auto.commit':False})
                try:
                    assert reader.committed([TopicPartition(topic,0)],timeout=10)[0].offset==n
                finally:
                    reader.close()
                with sqlite3.connect(db) as archive:
                    normalized_bytes=archive.execute('SELECT COALESCE(SUM(LENGTH(CAST(normalized_json AS BLOB))),0) FROM records').fetchone()[0]
                archive.close()
                return db,dict(database_bytes=db.stat().st_size,normalized_json_bytes=normalized_bytes,language=language,label=label,seconds=seconds,messages_per_second=n/seconds,
                               active_seconds=active,active_messages_per_second=n/active if active else None,
                               client_cpu_seconds=after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime)

            def remove_database(path):
                for suffix in ('','-wal','-shm'):
                    Path(str(path)+suffix).unlink(missing_ok=True)
            for language in languages:
                db,_=run(language,'warmup',args.warmup)
                remove_database(db)
            runs=[]
            for i in range(args.rounds):
                measured={}
                order=(('python','rust') if i%2==0 else ('rust','python')) if not args.before_root else [
                    ('python-before','python','rust-before','rust'),('python','rust','python-before','rust-before'),
                    ('rust','rust-before','python','python-before'),('rust-before','python-before','rust','python')][i%4]
                for language in order:
                    measured[language]=run(language,'round-'+str(i+1),count)
                parity=compare_databases(measured['python'][0],measured['rust'][0])
                assert parity['records']==count
                if args.before_root:
                    old=compare_databases(measured['python-before'][0],measured['rust-before'][0])
                    assert old['records']==count
                    assert compare_databases(measured['python-before'][0],measured['python'][0],compare_transport=False)['records']==count
                    assert compare_databases(measured['rust-before'][0],measured['rust'][0],compare_transport=False)['records']==count
                # 同时证明留档的 value 顺序与实际输入逐字节一致。
                import base64
                with sqlite3.connect(measured['python'][0]) as db:
                    archived=db.execute('SELECT offset_id,raw_base64 FROM records ORDER BY offset_id')
                    for position,(offset,raw) in enumerate(archived):
                        assert offset==position and base64.b64decode(raw)==payloads[position]
                runs.extend(value[1] for value in measured.values())
                print(json.dumps(dict(topic=topic,round=i+1,parity='PASS',
                                      **{lang:round(value[1]['messages_per_second']) for lang,value in measured.items()})),flush=True)
                for db,_ in measured.values():
                    remove_database(db)
            summary={}
            for language in languages:
                values=[run for run in runs if run['language']==language]
                summary[language]={key:statistics.median(row[key] for row in values) for key in
                                   ('messages_per_second','active_messages_per_second','client_cpu_seconds','database_bytes','normalized_json_bytes')}
            channels.append(dict(topic=topic,messages_per_run=count,input_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                                 source=str(source.relative_to(ROOT)),summary=summary,runs=runs,parity=parity))
    assert initial_code==source_digest(),'测量期间应用代码变化'
    report=dict(before_source_code_sha256=before_manifest['source_code_sha256'] if before_manifest else None,
                before_rust_binary_sha256=before_manifest['rust_binary_sha256'] if before_manifest else None,
                replay_keys='valid source MMSI as UTF8, invalid rows have null key; local replay keys, not claimed original upstream keys',
                result='PASS',date_CN=stamp,source_code_sha256=initial_code,native_versions=versions,
                rust_binary_sha256=hashlib.sha256(BINARY.read_bytes()).hexdigest(),rounds=args.rounds,
                warmup_messages_per_language=args.warmup,batch_size=2048,batch_max_wait_ms=50,
                input='all actual bridge export rows once per run, including actual invalid rows; no generated business values',
                conditions='isolated Kafka 4.2.2; one broker/partition/replica; one worker; SQLite WAL+FULL; persist then sync offset commit',
                timing='process start to exit; producer seeding and parent verification excluded',
                limits='vessel projections, not original production Kafka value; local backlog replay, not production latency or broker capacity',channels=channels)
    target=folder/'results.json'
    target.write_text(json.dumps(report,ensure_ascii=False,indent=2))
    latest=folder.parent.parent/'continuous-ais-latest.json'
    latest.unlink(missing_ok=True)
    latest.symlink_to(target.relative_to(latest.parent))
    for log in folder.glob('*.log'):
        log.unlink()  # 全部核验成功后仅保留正式报告；失败运行的日志仍可排查。
    print(json.dumps(dict(result='PASS',report=str(target)),ensure_ascii=False))

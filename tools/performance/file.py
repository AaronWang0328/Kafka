"""Full same-input Python/Rust file-to-SQLite performance; no Kafka/network measurement."""
import argparse
from datetime import datetime
import fcntl
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

from confluent_kafka import libversion
from bridge_kafka.offline import jsonl_records
from bridge_kafka.settings import ROOT
from bridge_kafka.supplement import PREPARED
from tools.common import CHANNELS,write_config
from tools.common import BINARY
from tools.validation.compare import compare_databases


def code_digest():
    digest=hashlib.sha256()
    for p in [*sorted((ROOT/'src/python/bridge_kafka').glob('*.py')),*sorted((ROOT/'src/rust/src').glob('*.rs'))]:digest.update(p.read_bytes())
    return digest.hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rounds',type=int,default=4)
    parser.add_argument('--warmup',type=int,default=5000)
    args=parser.parse_args()
    assert args.rounds>=2 and args.warmup>=1
    versions=json.loads(subprocess.check_output([str(BINARY),'--versions'],text=True))
    assert versions=={'librdkafka':libversion()[0],'sqlite':sqlite3.sqlite_version}
    source_code=code_digest()
    manifest=json.loads((PREPARED/'manifest.json').read_text())
    stamp=datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    folder=ROOT/'reports/performance/ais-offline'/stamp;folder.mkdir(parents=True)
    channels=[]
    with open('/tmp/changtai-kafka-test.lock','a') as lock,tempfile.TemporaryDirectory(prefix='ct-ais-file-bench-') as temporary:
        fcntl.flock(lock,fcntl.LOCK_EX)
        work=Path(temporary)
        for topic,config in CHANNELS:
            source=PREPARED/(topic+'.jsonl');count=manifest['topic_rows'][topic]
            warmup=work/(topic+'.warmup.jsonl')
            with warmup.open('wb') as output:
                for i,(_,raw) in enumerate(jsonl_records(source)):
                    if i>=args.warmup:break
                    output.write(raw+b'\n')
            def run(language,label,path,n):
                database=work/(topic+'-'+language+'-'+label+'.db')
                if language=='python':
                    cfg=work/(topic+'.toml');write_config(cfg,config,'127.0.0.1:9092','offline',database)
                    cmd=[sys.executable,str(ROOT/'manage.py'),'python','--config',str(cfg),'import-file','--file',str(path)]
                else:
                    cmd=[str(BINARY),'--topic',topic,'--jsonl',str(path),'--database',str(database)]
                before=resource.getrusage(resource.RUSAGE_CHILDREN);start=time.perf_counter()
                logfile=folder/(topic+'-'+label+'-'+language+'.log')
                with logfile.open('wb') as log:result=subprocess.run(cmd,cwd=ROOT,stdout=log,stderr=log,timeout=240)
                seconds=time.perf_counter()-start;after=resource.getrusage(resource.RUSAGE_CHILDREN)
                metrics=None
                for line in logfile.read_text().splitlines():
                    if line.startswith('{'):metrics=json.loads(line)
                assert metrics is not None,logfile
                invalid=metrics['invalid']
                assert result.returncode==(1 if invalid else 0),logfile
                assert metrics.get('messages',metrics.get('processed'))==n
                return database,{'language':language,'round':label,'messages':n,'invalid':invalid,'seconds':seconds,
                    'messages_per_second':n/seconds,'client_cpu_seconds':after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime}
            for language in ('python','rust'):
                db,_=run(language,'warmup',warmup,min(args.warmup,count))
                for suffix in ('','-wal','-shm'):Path(str(db)+suffix).unlink(missing_ok=True)
            runs=[]
            for i in range(args.rounds):
                measured={}
                for language in (('python','rust') if i%2==0 else ('rust','python')):
                    measured[language]=run(language,'round-'+str(i+1),source,count)
                parity=compare_databases(measured['python'][0],measured['rust'][0]);assert parity['records']==count
                runs.extend(v[1] for v in measured.values())
                print(json.dumps({'topic':topic,'round':i+1,'parity':'PASS',**{k:round(v[1]['messages_per_second']) for k,v in measured.items()}}),flush=True)
                for db,_ in measured.values():
                    for suffix in ('','-wal','-shm'):Path(str(db)+suffix).unlink(missing_ok=True)
            summary={}
            for language in ('python','rust'):
                values=[r for r in runs if r['language']==language]
                summary[language]={k:statistics.median(r[k] for r in values) for k in ('messages_per_second','client_cpu_seconds')}
            channels.append({'topic':topic,'messages_per_run':count,'input_sha256':manifest['output_sha256'][topic+'.jsonl'],
                             'summary':summary,'parity':parity,'runs':runs})
    assert source_code==code_digest(),'application source changed during benchmark'
    report={'result':'PASS','mode':'OFFLINE_FILE_TO_SQLITE','date_CN':stamp,'source_code_sha256':source_code,
            'native_versions':versions,'rounds':args.rounds,'warmup_messages_per_language':args.warmup,
            'batch_size':2048,'durability':'SQLite WAL+FULL, one application worker; actual invalid rows archived',
            'input':'all 268615 bridge projection rows once per round, no synthetic rows and no five-message loop',
            'timing':'process launch to exit; input SHA256, parsing, raw archive, state merge and database close included; parent comparison excluded',
            'comparison':'all raw bytes, source identities, status, errors, normalized results and ship snapshots; received_at excluded; no numeric tolerance',
            'limits':'FILE INPUT ONLY; no Kafka delivery/receive/offset commit, no live latency, Broker capacity or production SLA',
            'channels':channels}
    path=folder/'results.json';path.write_text(json.dumps(report,ensure_ascii=False,indent=2))
    latest=ROOT/'reports/performance/ais-offline-latest.json'
    latest.unlink(missing_ok=True)
    latest.symlink_to(path.relative_to(latest.parent))
    for log in folder.glob('*.log'):
        log.unlink()  # 成功报告已包含逐轮数据，失败运行仍保留过程日志。
    print(json.dumps({'result':'PASS','report':str(path),'channels':[{'topic':c['topic'],'summary':c['summary']} for c in channels]},ensure_ascii=False,indent=2))


if __name__=='__main__':main()

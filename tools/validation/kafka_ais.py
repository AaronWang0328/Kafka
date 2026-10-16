"""新增 AIS 两来源的 Kafka 重启、去重与 CSV 独有观测端到端核对。"""
import base64
import sqlite3
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from confluent_kafka import Consumer,TopicPartition
from bridge_kafka.cli import init_topic
from bridge_kafka.model import InvalidMessage, normalize
from bridge_kafka.store import Store
from bridge_kafka.offline import jsonl_records
from bridge_kafka.settings import ROOT,load_settings
from bridge_kafka.supplement import PREPARED
from tools.common import CHANNELS,write_config
from tools.common import BINARY
from tools.validation.cluster import local_cluster
from tools.validation.compare import compare_databases


def main():
    cases=[]
    with local_cluster() as bootstrap,tempfile.TemporaryDirectory(prefix='ct-ais-resume-') as temporary:
        work=Path(temporary)
        for topic,config in CHANNELS:
            settings=load_settings(ROOT/config);selected=[]
            for _,raw in jsonl_records(PREPARED/(topic+'.jsonl')):
                if len(selected)<15:selected.append(raw)
                elif len(str(json.loads(raw)['mmsi']))!=9:selected.append(raw);break
            selected += [raw for _,raw in jsonl_records(PREPARED/(topic+'.csv-extra.jsonl'))]
            count=len(selected);source=work/(topic+'.jsonl');source.write_bytes(b'\n'.join(selected)+b'\n')
            init_topic({**settings,'client':{**settings['client'],'bootstrap.servers':bootstrap}})
            cfg=work/(topic+'.toml');write_config(cfg,config,bootstrap,'seed',work/'unused.db')
            seed=subprocess.run([sys.executable,str(ROOT/'manage.py'),'python','--config',str(cfg),'produce','--file',str(source),'--interval','0','--allow-invalid'],cwd=ROOT,capture_output=True,timeout=60)
            assert seed.returncode==0,seed.stderr.decode()[-1500:]
            dbs={language:work/(topic+'-'+language+'.db') for language in ('python','rust')}
            def consume(language,group,n,expected):
                if language=='python':
                    write_config(cfg,config,bootstrap,group,dbs[language])
                    cmd=[sys.executable,str(ROOT/'manage.py'),'python','--config',str(cfg),'consume','--max-messages',str(n),'--idle-timeout','20']
                else:
                    cmd=[str(BINARY),'--bootstrap-servers',bootstrap,'--topic',topic,'--group',group,'--database',str(dbs[language]),'--count',str(n),'--idle-timeout','20']
                run=subprocess.run(cmd,cwd=ROOT,capture_output=True,timeout=60)
                assert run.returncode==0,run.stderr.decode()[-1500:]
                c=Consumer({'bootstrap.servers':bootstrap,'group.id':group,'enable.auto.commit':False})
                try:assert c.committed([TopicPartition(topic,0)],timeout=10)[0].offset==expected
                finally:c.close()
            for language in dbs:
                group=topic+'-'+language
                consume(language,group,2,2);consume(language,group,count-2,count)
            parity=compare_databases(dbs['python'],dbs['rust'])
            assert parity['records']==count and parity['statuses']['invalid']>=1
            for language in dbs:consume(language,topic+'-'+language+'-replay',count,count)
            assert compare_databases(dbs['python'],dbs['rust'])==parity
            store=Store(dbs['python'])
            try: quality=store.quality_report()
            finally: store.close()
            rust_quality=json.loads(subprocess.check_output([str(BINARY),'--quality','--database',str(dbs['rust'])],text=True))
            assert quality==rust_quality and quality['transport_metadata_records']==count
            with sqlite3.connect(dbs['python']) as db:
                rows=db.execute('SELECT kafka_key_base64,kafka_headers_json FROM records ORDER BY offset_id').fetchall()
            for raw,(key,headers) in zip(selected,rows):
                try: event=normalize(raw,settings['input'])
                except InvalidMessage: expected=None
                else: expected=base64.b64encode(event['mmsi'].encode('ascii')).decode('ascii')
                assert key==expected and json.loads(headers)==[]
            cases.append({'topic':topic,'quality':quality,'quality_parity':'PASS','actual_replay_key_parity':'PASS',**parity,'offset_resume':'PASS','replay_dedup':'PASS'})
    report={'result':'PASS','scope':'two AIS topics; original export examples, invalid MMSI, all 146 CSV extras',
            'test_cluster':'isolated loopback, destroyed after test','channels':cases}
    (ROOT/'reports/validation').mkdir(parents=True,exist_ok=True)
    (ROOT/'reports/validation/ais-kafka-parity.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps(report,ensure_ascii=False,indent=2))


if __name__=='__main__':main()

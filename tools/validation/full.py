"""Full real-data import, diagnostics and replay comparison; temporary databases are cleaned by default."""
import argparse
import tempfile
from datetime import datetime
import hashlib
import json
from zoneinfo import ZoneInfo
from pathlib import Path
import subprocess
import sys

from bridge_kafka.store import Store
from bridge_kafka.offline import import_file
from bridge_kafka.settings import ROOT,load_settings
from bridge_kafka.supplement import PREPARED,prepare
from tools.validation.compare import compare_databases
from tools.common import BINARY


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--keep-databases',action='store_true',help='显式保留本次验证库；默认自动清理')
    parser.add_argument('--reference-report',type=Path,help='比较融合前全量业务语义哈希，不要求保留旧库')
    args=parser.parse_args()
    references = {c['topic']:c for c in json.loads(args.reference_report.read_text())['channels']} if args.reference_report else {}
    temporary = tempfile.TemporaryDirectory(prefix='ct-full-real-')
    output = ROOT/'var/verification'/datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S') if args.keep_databases else Path(temporary.name)
    manifest=prepare()
    channels=[]
    for topic,config in [('ais_parsing_ori','configs/ais-origin.toml'),('REALTIME_TOPIC','configs/ais-platform.toml')]:
        settings=load_settings(ROOT/config)
        databases={'python':output/(topic+'.sqlite3'),'rust':output/'rust'/(topic+'.sqlite3')}
        summary={}
        for language in ('python','rust'):
            for suffix in ('.jsonl','.csv-extra.jsonl'):
                path=PREPARED/(topic+suffix)
                if language=='python':
                    result=import_file({**settings,'database':databases[language]},path)
                    summary[suffix]=result
                else:
                    completed=subprocess.run([str(BINARY),'--topic',topic,'--jsonl',str(path),'--database',str(databases[language])],
                        cwd=ROOT,capture_output=True,text=True,timeout=180)
                    expected_code=1 if summary[suffix]['invalid'] else 0
                    assert completed.returncode==expected_code,completed.stderr[-1500:]
            print(json.dumps({'topic':topic,'language':language,'import':'complete'}),flush=True)
        parity=compare_databases(databases['python'],databases['rust'])
        if references:
            assert parity['semantic_sha256']==references[topic]['semantic_sha256'], 'business results changed against reference'
        assert parity['records']==manifest['topic_rows'][topic]+manifest['csv_extra_rows'][topic]
        # Re-import every source, not just a sample: row identity and projections must stay unchanged.
        for suffix in ('.jsonl','.csv-extra.jsonl'):
            path=PREPARED/(topic+suffix)
            replay=import_file({**settings,'database':databases['python']},path)
            assert replay['inserted']==0 and replay['duplicates']==replay['messages']
            completed=subprocess.run([str(BINARY),'--topic',topic,'--jsonl',str(path),'--database',str(databases['rust'])],
                cwd=ROOT,capture_output=True,text=True,timeout=180)
            assert completed.returncode==(1 if summary[suffix]['invalid'] else 0)
        assert compare_databases(databases['python'],databases['rust'])==parity
        store=Store(databases['python'])
        try: quality=store.quality_report()
        finally: store.close()
        rust_quality=json.loads(subprocess.check_output([str(BINARY),'--quality','--database',str(databases['rust'])],text=True))
        assert quality==rust_quality, 'quality diagnostics differ'
        channels.append({'topic':topic,'quality':quality,'quality_parity':'PASS','reference_business_parity':'PASS' if references else None,**parity,'full_replay_dedup':'PASS','databases':{k:str(v) for k,v in databases.items()}})
        print(json.dumps(channels[-1],ensure_ascii=False),flush=True)
    code=hashlib.sha256()
    for p in [*sorted((ROOT/'src/python/bridge_kafka').glob('*.py')),*sorted((ROOT/'src/rust/src').glob('*.rs'))]:code.update(p.read_bytes())
    report={'result':'PASS','fresh_databases':True,'databases_retained':args.keep_databases,'source_code_sha256':code.hexdigest(),'scope':'entire supplement JSONL plus non-overlapping CSV extras',
            'bridge_rows':manifest['bridge_rows'],'csv_extra_rows':sum(manifest['csv_extra_rows'].values()),
            'csv_overlap_excluded':manifest['csv_overlapping_rows'],'channels':channels}
    (ROOT/'reports/validation').mkdir(parents=True,exist_ok=True)
    (ROOT/'reports/validation/ais-offline-parity.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps(report,ensure_ascii=False,indent=2))
    temporary.cleanup()


if __name__=='__main__':main()

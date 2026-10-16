"""按实际 Topic 拆分逐船导出；CSV 只补不在 JSONL 中的观测，不伪造采集时间。"""
import argparse
import csv
import hashlib
import json
from pathlib import Path

from .continuous import TOPICS
from .offline import jsonl_records
from .settings import ROOT

SOURCE = ROOT / 'data/source/ct_offline_ais_supplement_20261015'
PREPARED = ROOT / 'data/prepared/ais-supplement'


def digest(path):
    with Path(path).open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def row_key(row, csv=False):
    return (str(row['mmsi']), row['recv'], row['topic'],
            *[float(row[key]) for key in ('lon','lat','speed_kn' if csv else 'speed',
                                          'course_deg' if csv else 'course','ship_length','ship_width')])


def prepare(source=SOURCE, output=PREPARED):
    source, output = Path(source), Path(output)
    output.mkdir(parents=True, exist_ok=True)
    tracks = list(csv.DictReader((source/'ais_alarm_mmsi_tracks.csv').open()))
    wanted = {row_key(row, True) for row in tracks}
    matched = set()
    counts = {topic: 0 for topic in TOPICS}
    handles = {topic: (output/(topic+'.jsonl.tmp')).open('wb') for topic in TOPICS}
    indices = {topic: (output/(topic+'.source-lines.jsonl.tmp')).open('w') for topic in TOPICS}
    try:
        for line, raw in jsonl_records(source/'ais_window_bridge.jsonl'):
            row = json.loads(raw)
            topic = row['topic']
            if topic not in TOPICS:
                raise ValueError('补充包出现未确认的 Topic')
            handles[topic].write(raw+b'\n')
            counts[topic] += 1
            indices[topic].write(json.dumps({'prepared_line':counts[topic], 'source_line':line})+'\n')
            key = row_key(row)
            if key in wanted:
                matched.add(key)
    finally:
        for h in [*handles.values(), *indices.values()]:
            h.close()
    for topic in TOPICS:
        for suffix in ('.jsonl', '.source-lines.jsonl'):
            (output/(topic+suffix+'.tmp')).replace(output/(topic+suffix))
    extras = {topic: [] for topic in TOPICS}
    duplicates = 0
    seen = set(matched)
    for line, row in enumerate(tracks, 2):
        key = row_key(row, True)
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        if row['topic'] not in TOPICS:
            raise ValueError('轨迹 CSV 出现未确认的 Topic')
        extras[row['topic']].append({'format':'ct_ais_track_csv', 'topic':row['topic'], 'csv_row':row,
                                   'source_file':'ais_alarm_mmsi_tracks.csv', 'source_line':line})
    for topic, rows in extras.items():
        target = output/(topic+'.csv-extra.jsonl')
        temporary = target.with_name(target.name+'.tmp')
        temporary.write_text(''.join(json.dumps(r,ensure_ascii=False)+'\n' for r in rows))
        temporary.replace(target)
    report = {'result':'PASS', 'input':str(source), 'output':str(output),
              'source_sha256':{p.name:digest(p) for p in sorted(source.iterdir()) if p.is_file()},
              'bridge_rows':sum(counts.values()), 'topic_rows':counts,
              'csv_rows':len(tracks), 'csv_overlapping_rows':duplicates,
              'csv_extra_rows':{k:len(v) for k,v in extras.items()},
              'output_sha256':{p.name:digest(p) for p in sorted(output.glob('*.jsonl'))},
              'semantics':'JSONL split without rewriting row bytes; source line indices retained. CSV extras explicitly wrapped with original columns and line; AIS acquisition time unknown. Not original production Kafka value bytes.'}
    (output/'manifest.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',type=Path,default=SOURCE)
    parser.add_argument('--output',type=Path,default=PREPARED)
    args=parser.parse_args()
    print(json.dumps(prepare(args.source,args.output),ensure_ascii=False,indent=2))


if __name__ == '__main__':
    main()

"""项目路径和真实交付来源；不生成业务数据。"""
from bridge_kafka.settings import ROOT, load_settings

BINARY = ROOT / ".runtime/rust-target/release/bridge-kafka"
AIS_CHANNELS = [('ais_parsing_ori','configs/ais-origin.toml'),('REALTIME_TOPIC','configs/ais-platform.toml')]
CHANNELS = AIS_CHANNELS
SITE_CHANNELS = [('configs/alarm.toml','real_alarm_raw.jsonl'),('configs/media.toml','down_pic.jsonl'),
                 ('configs/media-plus.toml','down_pic_plus.jsonl')]
SITE_SOURCE = ROOT / 'data/source/ct_offline_batch2_20261015/pkg2/01_alarm'


def write_config(path, config, bootstrap, group, database):
    settings = load_settings(ROOT/config)
    text = (ROOT/config).read_text().replace('bootstrap_servers = "127.0.0.1:9092"', f'bootstrap_servers = "{bootstrap}"')
    text = text.replace(f'group_id = "{settings["group"]}"', f'group_id = "{group}"')
    text = text.replace(f'database = "{settings["database"].relative_to(ROOT)}"', f'database = "{database}"')
    path.write_text(text)


def capture_database(path):
    import json
    import sqlite3
    with sqlite3.connect(path) as db:
        records = []
        for row in db.execute('SELECT topic,partition_id,offset_id,kafka_timestamp_ms,raw_base64,status,normalized_json,raw_is_null FROM records ORDER BY topic,partition_id,offset_id'):
            records.append([*row[:6],json.loads(row[6]) if row[6] else None,row[7]])
        ships = [[m,json.loads(d) if d else None,json.loads(s) if s else None]
                 for m,d,s in db.execute('SELECT mmsi,dynamic_json,static_json FROM latest_ship ORDER BY mmsi')]
    return dict(records=records,ships=ships)

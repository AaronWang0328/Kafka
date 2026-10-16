"""留档契约与确定性质量统计；来源标识不充当上游业务事件 ID。"""
import json
from .model import BUSINESS_FIELDS, events_of

RECORD_SCHEMA = 'bridge-kafka.record.v1'


def record_id(topic, partition, offset):
    return f'v1:{topic}:{partition}:{offset}'


def quality_report(db):
    statuses = {'valid': 0, 'invalid': 0}
    statuses.update(db.execute('SELECT status,COUNT(*) FROM records GROUP BY status'))
    result = {'schema_version': 'bridge-kafka.quality.v1', 'records': sum(statuses.values()),
              'statuses': statuses, 'ships': db.execute('SELECT COUNT(*) FROM latest_ship').fetchone()[0],
              'transport_metadata_records': db.execute('SELECT COUNT(*) FROM records WHERE kafka_headers_json IS NOT NULL').fetchone()[0],
              'ais_events': 0, 'media_commands': 0, 'known_ais_times': 0, 'unknown_ais_times': 0,
              'unconfirmed_coordinate_system_events': 0, 'missing_fields': dict.fromkeys(BUSINESS_FIELDS, 0),
              'issues': {}, 'position_gate_rejections': {}}
    for (normalized,) in db.execute('SELECT normalized_json FROM records WHERE normalized_json IS NOT NULL'):
        for event in events_of(json.loads(normalized)):
            if event['message_type'] == 'media_command':
                result['media_commands'] += 1
                continue
            result['ais_events'] += 1
            result['known_ais_times' if event.get('event_time') is not None else 'unknown_ais_times'] += 1
            result['unconfirmed_coordinate_system_events'] += event.get('source', {}).get('coordinate_system') == 'unknown'
            for name in BUSINESS_FIELDS:
                result['missing_fields'][name] += event['payload'].get(name) is None
            for field in ('issues', 'position_gate_rejections'):
                for reason in event.get('quality', {}).get(field, []):
                    result[field][reason] = result[field].get(reason, 0) + 1
    result['first_invalid_records'] = [dict(record_id=record_id(t, p, o), topic=t, partition=p, offset=o, error=e)
        for t,p,o,e in db.execute("SELECT topic,partition_id,offset_id,error FROM records WHERE status='invalid' ORDER BY topic,partition_id,offset_id LIMIT 10")]
    return result

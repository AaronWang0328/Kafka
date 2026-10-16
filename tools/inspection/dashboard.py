"""只读现有 SQLite 与验证报告；展示真实记录，不产生或发布业务消息。"""
import argparse
import base64
from collections import deque
from datetime import datetime
import hashlib
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
from urllib.parse import unquote, urlsplit
import tomllib

from bridge_kafka.diagnostics import quality_report, record_id
from bridge_kafka.model import BUSINESS_FIELDS, events_of
from bridge_kafka.settings import ROOT
from tools.validation.compare import compare_databases

TOPICS = [('ais_parsing_ori', 'ais-origin.toml'), ('REALTIME_TOPIC', 'ais-platform.toml'),
          ('ABNORMAL_TOPIC', 'alarm.toml'), ('down_pic', 'media.toml'), ('down_pic_plus', 'media-plus.toml')]
OUT = ROOT / 'reports/dashboard'
REQUIREMENTS = 'docs/requirements/常泰桥梁_功能需求说明.html'
REPORTS = ['reports/validation/ais-offline-parity.json', 'reports/validation/ais-kafka-parity.json',
           'reports/validation/site-protocols-integration.json', 'reports/fusion/peer-integration-20261016.json']
RAW_FIELDS = ('recv', 'topic', 'mmsi', 'name', 'lon', 'lat', 'speed', 'course', 'heading',
              'ship_length', 'ship_width', 'last_updated', 'source_file', 'source_line')


def digest_source():
    digest = hashlib.sha256()
    for path in [*sorted((ROOT/'src/python/bridge_kafka').glob('*.py')), *sorted((ROOT/'src/rust/src').glob('*.rs'))]:
        digest.update(path.read_bytes())
    return digest.hexdigest()


def load_report(path, source_hash):
    file = ROOT/path
    data = json.loads(file.read_text())
    return {'path': path, 'sha256': hashlib.sha256(file.read_bytes()).hexdigest(),
            'modified': datetime.fromtimestamp(file.stat().st_mtime).astimezone().isoformat(timespec='seconds'),
            'code_matches': data.get('source_code_sha256') == source_hash if 'source_code_sha256' in data else None,
            'data': data}


def safe_event(event):
    """明确的展示字段白名单；媒体 URL、凭证及原文不进入网页。"""
    source = event.get('source', {})
    payload = event.get('payload', {})
    shown = {key: payload[key] for key in (*BUSINESS_FIELDS, 'picture_path', 'video_path', 'option', 'option_semantics') if key in payload}
    return {'mmsi': event.get('mmsi'), 'message_type': event.get('message_type'),
            'event_time': event.get('event_time'), 'payload': shown,
            'source': {key: source[key] for key in ('kind', 'topic', 'time_basis', 'received_time', 'observation_time',
                       'coordinate_system', 'reported_update_age_seconds', 'heading_deg', 'event_id') if key in source},
            'quality': event.get('quality', {}),
            'alarm': {key: event['alarm'][key] for key in ('warn_type', 'warn_time', 'event_id', 'info', 'grade')
                      if key in event.get('alarm', {})},
            'capture': {key: event['capture'][key] for key in ('topic', 'partition', 'offset', 'kafka_ts', 'recv')
                        if key in event.get('capture', {})},
            'camera_credentials_present': payload.get('credentials_present')}


def snapshots(db):
    result = {}
    for mmsi, dynamic, static in db.execute('SELECT mmsi,dynamic_json,static_json FROM latest_ship ORDER BY mmsi'):
        fields, times = dict.fromkeys(BUSINESS_FIELDS), {}
        for value in (static, dynamic):
            if value:
                obj = json.loads(value)
                fields.update({k: v for k, v in obj['payload'].items() if k in BUSINESS_FIELDS})
                times.update(obj.get('field_times', {}))
        result[mmsi] = {'payload': fields, 'field_times': times}
    return result


def open_readonly(path):
    db = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True)
    db.execute('BEGIN')
    return db


def current_quality(db):
    """旧库缺传输元数据列时只读适配；不执行迁移或补写历史数据。"""
    columns = {r[1] for r in db.execute('PRAGMA table_info(records)')}
    if 'kafka_headers_json' in columns:
        return quality_report(db)

    class LegacyReader:
        def execute(self, sql):
            return db.execute(sql.replace('kafka_headers_json IS NOT NULL', 'NULL IS NOT NULL'))

    return quality_report(LegacyReader())


def inspect_channel(topic, config_name):
    config = tomllib.loads((ROOT/'configs'/config_name).read_text())
    assert config['kafka']['topic'] == topic
    py_path, rs_path = ROOT/config['storage']['database'], ROOT/'var/rust'/f'{topic}.sqlite3'
    def file_versions():
        return [(str(file), file.stat().st_size, file.stat().st_mtime_ns)
                for path in (py_path, rs_path) for file in (path, Path(str(path)+'-wal')) if file.exists()]
    versions = file_versions()
    print(f'只读核对 {topic}', flush=True)
    try:
        parity = {'result': 'PASS', **compare_databases(py_path, rs_path, read_only=True)}
    except (AssertionError, sqlite3.Error, OSError) as exc:
        parity = {'result': 'FAIL', 'error': str(exc)}
    py, rs = open_readonly(py_path), open_readonly(rs_path)
    try:
        quality = current_quality(py)
        py_ships, rs_ships = snapshots(py), snapshots(rs)
        ships = {m: {'python': value, 'rust': rs_ships.get(m), 'samples': [], 'count': 0} for m, value in py_ships.items()}
        tails, invalid, commands = {}, [], []
        sql = 'SELECT topic,partition_id,offset_id,status,error,normalized_json,raw_base64 FROM records ORDER BY topic,partition_id,offset_id'
        for t, p, o, status, error, normalized, raw in py.execute(sql):
            row = {'record_id': record_id(t, p, o), 'archive_topic': t, 'partition': p, 'offset': o,
                   'status': status, 'error': error, 'raw_sha256': hashlib.sha256(base64.b64decode(raw)).hexdigest()}
            if status == 'invalid':
                if len(invalid) < 10:
                    original = json.loads(base64.b64decode(raw)) if topic in ('ais_parsing_ori', 'REALTIME_TOPIC') else {}
                    invalid.append({**row, 'input_mmsi': original.get('mmsi')})
                continue
            for event in events_of(json.loads(normalized)):
                mmsi = event.get('mmsi')
                item = {**row, 'event': safe_event(event)}
                if topic in ('ais_parsing_ori', 'REALTIME_TOPIC'):
                    original = json.loads(base64.b64decode(raw))
                    item['input'] = {k: original[k] for k in RAW_FIELDS if k in original}
                if mmsi in ships:
                    state = ships[mmsi]
                    state['count'] += 1
                    if len(state['samples']) < 4:
                        state['samples'].append(item)
                    else:
                        tails.setdefault(mmsi, deque(maxlen=4)).append(item)
                elif event['message_type'] == 'media_command':
                    commands.append(item)
        for m, tail in tails.items():
            ships[m]['samples'].extend(tail)
        if versions != file_versions():
            raise RuntimeError(f'{topic} 生成期间数据库改变，请暂停写入后重新生成')
        return {'topic': topic, 'config': 'configs/'+config_name, 'python_database': str(py_path.relative_to(ROOT)),
                'rust_database': str(rs_path.relative_to(ROOT)), 'quality': quality, 'parity': parity,
                'ships': ships, 'commands': commands, 'invalid': invalid,
                'thresholds': config.get('quality', {}), 'batch': config['processing']}
    finally:
        py.close()
        rs.close()


class RequirementsText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts, self.skip = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in ('style', 'script', 'svg'):
            self.skip += 1

    def handle_endtag(self, tag):
        if tag in ('style', 'script', 'svg'):
            self.skip -= 1

    def handle_data(self, data):
        if not self.skip:
            self.parts.append(data)


def requirements(verified):
    # 每项都带原文短句；下面的状态只针对本模块及本地证据。
    rows = [
        ('1.2 / 4.2', '持续接入 AIS 身份、尺度与动态位置', 'Kafka 消息流', 'local',
         '两个真实来源独立接入，AIS 持续处理，不以告警为开关。原文和字段时间留档。',
         '本地回放通过；现网连续 AIS 的原始 value、连接及权限仍需联调。', 'src/python/bridge_kafka/continuous.py'),
        ('3.2 / 3.5', '订阅告警，保留业务标识、时间与位置', '常驻订阅告警事件', 'local',
         'ABNORMAL_TOPIC 告警与持续 AIS 分开处理；down_pic、down_pic_plus 作为媒体指令，不混入 AIS。',
         '可验接收、归档和重放；相机选择、检测跟踪、录制由媒体模块验收。', 'src/python/bridge_kafka/site_protocols.py'),
        ('4.1 / 4.6', '来源可追查，为结果留档提供输入证据', '可追责的结论', 'local',
         '原文、来源位置、规范化结果和异常均留档；持久化后提交进度。两版当前库逐条比较。',
         '这验证的是输入追溯与恢复；点云、测高结果留档尚需下游实现。', 'src/python/bridge_kafka/store.py'),
        ('1.2 / 4.2', '补齐并确认 AIS 关键字段', '登记长/宽/型深', 'pending',
         '缺失型深保持未知；区分确认的 AIS 采集时间和平台 createDate。坐标基准标为未知。',
         '补充型深或确认其使用范围，确认坐标基准和平台位置采集时刻，不能靠代码猜。', 'src/python/bridge_kafka/diagnostics.py'),
        ('4.3 / 4.6', '按可信的位置与时间关联雷达目标', '宁可不匹配也不误匹配', 'partial',
         'AIS 侧支持位置范围、过期和未来时差门限，并标记可信条件；现有门限未获现场确认。',
         '确认时钟、坐标、空间范围和匹配门限；完成雷达目标关联并检查错配/漏配。', 'src/python/bridge_kafka/quality.py'),
        ('3.3 / 3.5', '输出实际可播放的标注视频和快照', '文件真实存在且非空', 'external',
         '当前仅解析告警/媒体指令并留档，不生成 JPG/MP4，不把包内视频算作本程序产物。',
         '独立验收真实相机取流、标定、目标框、约 20 秒录像、文件字节数和 HTTP 访问。', REQUIREMENTS),
        ('4.2 / 4.3 / 4.6', '三维成像、测高、登记尺度交叉校验', '输出最终高度', 'external',
         '当前输出 AIS 船舶状态供后续融合；没有以船宽比例推测船高。',
         '点云聚类跟踪、雷达/水面基准标定、多帧测量、AIS/VTS 关联后再验高度误差。', REQUIREMENTS),
        ('4.6', '实时展示融合结果与目标点云', '提供实时可视化数据', 'external',
         '本页展示 Kafka 模块的真实离线结果与证据，未连接现网，也没有融合点云画面。',
         '接通真实数据与下游融合输出后，再做系统实时效果和长跑验收。', REQUIREMENTS),
    ]
    parser = RequirementsText()
    parser.feed((ROOT/REQUIREMENTS).read_text())
    text = ''.join(parser.parts)
    result = []
    for section, name, excerpt, state, implemented, acceptance, code in rows:
        assert excerpt in text, f'原始需求短句不匹配: {excerpt}'
        if state == 'local' and not verified:
            state = 'pending'
        result.append(dict(section=section, name=name, excerpt=excerpt, state=state,
                           implemented=implemented, acceptance=acceptance, code=code))
    return result


def generate():
    code = digest_source()
    reports = [load_report(p, code) for p in REPORTS]
    performance_path = reports[-1]['data']['performance_report']
    reports.append(load_report(performance_path, code))
    channels = [inspect_channel(*topic) for topic in TOPICS]
    current_pass = all(c['parity']['result'] == 'PASS' for c in channels)
    full, performance = reports[0], reports[-1]
    full_by_topic = {c['topic']: c for c in full['data']['channels']}
    for channel in channels[:2]:
        channel['matches_full_report'] = channel['parity'].get('semantic_sha256') == full_by_topic[channel['topic']]['semantic_sha256']
    payload = {'generated_at': datetime.now().astimezone().isoformat(timespec='seconds'), 'source_hash': code,
               'source_unchanged': digest_source() == code, 'requirements_path': REQUIREMENTS,
               'requirements_sha256': hashlib.sha256((ROOT/REQUIREMENTS).read_bytes()).hexdigest(),
               'channels': channels, 'reports': reports, 'performance': performance['data'],
               'requirements': requirements(current_pass and full['code_matches'] and full['data']['result'] == 'PASS')}
    if not payload['source_unchanged']:
        raise RuntimeError('生成期间源码改变，请重试')
    OUT.mkdir(parents=True, exist_ok=True)
    template = (Path(__file__).with_name('templates')/'dashboard.html').read_text()
    encoded = json.dumps(payload, ensure_ascii=False, separators=(',', ':'), allow_nan=False).replace('<', '\\u003c').replace('\u2028', '\\u2028').replace('\u2029', '\\u2029')
    (OUT/'index.html').write_text(template.replace('/*__REAL_DATA__*/null', encoded))
    receipt = {'generated_at': payload['generated_at'], 'source_code_sha256': code,
               'requirements_sha256': payload['requirements_sha256'],
               'current_databases_result': 'PASS' if current_pass else 'FAIL',
               'channels': [{k: c[k] for k in ('topic', 'python_database', 'rust_database', 'parity', 'matches_full_report') if k in c} for c in channels],
               'reports': [{k: v for k, v in r.items() if k != 'data'} for r in reports],
               'scope': 'read-only current database comparison; real first/last observations only, not live upstream or fusion output'}
    (OUT/'receipt.json').write_text(json.dumps(receipt, ensure_ascii=False, indent=2))
    return payload


def serve(port, payload):
    # 只向浏览器开放本页、报告、当前源码与需求；不提供数据库、原文包或相机凭证。
    allowed = {ROOT/REQUIREMENTS, OUT/'receipt.json'}
    allowed.update(ROOT/r['path'] for r in payload['reports'])
    allowed.update(p for base, pattern in ((ROOT/'src/python/bridge_kafka', '*.py'), (ROOT/'src/rust/src', '*.rs'), (ROOT/'configs', '*.toml')) for p in base.glob(pattern))
    allowed = {p.resolve() for p in allowed}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = unquote(urlsplit(self.path).path)
            file = OUT/'index.html' if path in ('/', '/index.html') else (ROOT/path.removeprefix('/')).resolve()
            if file != OUT/'index.html' and file not in allowed:
                self.send_error(404)
                return
            types = {'.html': 'text/html; charset=utf-8', '.json': 'application/json; charset=utf-8'}
            data = file.read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', types.get(file.suffix, 'text/plain; charset=utf-8'))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(data)))
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', port), Handler)
    print(f'可视化验收页 http://127.0.0.1:{server.server_port}/ （生成时快照；Ctrl+C 结束）', flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--serve', action='store_true', help='生成后仅在 127.0.0.1 提供页面及证据链接')
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()
    payload = generate()
    print(str(OUT/'index.html'), flush=True)
    if args.serve:
        serve(args.port, payload)


if __name__ == '__main__':
    main()

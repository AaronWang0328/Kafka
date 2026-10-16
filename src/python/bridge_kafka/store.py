"""原文留档、按 MMSI 汇总 HTML AIS 字段、先落盘再提交进度。"""
import base64
import json
import sqlite3

from .model import BUSINESS_FIELDS, DYNAMIC_FIELDS, STATIC_FIELDS, events_of, utc_now
from .quality import assess, check_quality_config, position_gate_rejections
from .diagnostics import RECORD_SCHEMA, record_id, quality_report

ENCODE_JSON = json.JSONEncoder(ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode


def field_times(event):
    if not event:
        return {}
    if "field_times" in event:
        return event["field_times"]
    return dict.fromkeys(event["payload"], event.get("event_time"))


def merge_snapshot(previous, event, kind, names, *, finalize=True):
    snapshot = (previous if isinstance(previous, dict) else json.loads(previous)) if previous else {"mmsi": event["mmsi"], "payload": {}}
    snapshot["field_times"] = field_times(snapshot)
    source_time = event.get("event_time")
    payload = event["payload"]
    times = snapshot["field_times"]
    sources = snapshot.get("field_sources", {})
    current_source = event.get("source", {})
    for name in names:
        if name not in payload:
            continue
        previous_time = times.get(name)
        if previous_time and (not source_time or source_time < previous_time):
            continue
        previous_source = sources.get(name, {})
        # 告警之间按告警时间排序，但不把该时间写进 AIS 采集时间。
        if (not previous_time and not source_time and previous_source.get("kind") == "alarm_snapshot" and
                current_source.get("kind") == "alarm_snapshot" and previous_source.get("observation_time") and
                (not current_source.get("observation_time") or
                 current_source["observation_time"] < previous_source["observation_time"])):
            continue
        if (not previous_time and not source_time and current_source.get("kind") in ("ais_export", "ais_track_csv") and
                previous_source.get("kind") in ("ais_export", "ais_track_csv") and
                previous_source.get("topic") == current_source.get("topic") and
                previous_source.get("observation_time") and
                current_source.get("observation_time", "") < previous_source["observation_time"]):
            continue
        snapshot["payload"][name] = payload[name]
        times[name] = source_time
        if current_source or "field_sources" in snapshot:
            sources = snapshot.setdefault("field_sources", {})
            if current_source:
                sources[name] = current_source
            else:
                sources.pop(name, None)
    snapshot["message_type"] = kind
    if finalize:
        finish_snapshot(snapshot)
    return snapshot


def finish_snapshot(snapshot):
    # 字段时间每次更新；聚合时间只在批次写出时计算，避免重复扫描同一船。
    snapshot["event_time"] = max((value for value in snapshot["field_times"].values() if value), default=None)


class Store:
    def __init__(self, path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS records (
                topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id INTEGER NOT NULL,
                received_at TEXT NOT NULL, kafka_timestamp_ms INTEGER,
                raw_base64 TEXT NOT NULL, status TEXT NOT NULL,
                error TEXT, normalized_json TEXT, raw_is_null INTEGER NOT NULL DEFAULT 0,
                kafka_key_base64 TEXT, kafka_headers_json TEXT,
                PRIMARY KEY(topic, partition_id, offset_id)
            );
            CREATE TABLE IF NOT EXISTS latest_ship (
                mmsi TEXT PRIMARY KEY, dynamic_time TEXT, dynamic_json TEXT,
                static_time TEXT, static_json TEXT
            );
        """)
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(records)")}
        if "raw_is_null" not in columns:
            self.db.execute("ALTER TABLE records ADD COLUMN raw_is_null INTEGER NOT NULL DEFAULT 0")
        for name in ("kafka_key_base64", "kafka_headers_json"):
            if name not in columns:
                self.db.execute(f"ALTER TABLE records ADD COLUMN {name} TEXT")
        self.db.commit()

    def save(self, topic, partition, offset, raw, event=None, error=None, timestamp=None, reprocess=False, quality_config=None, key=None, headers=None):
        return self.save_batch([{"topic": topic, "partition": partition, "offset": offset, "raw": raw,
            "event": event, "error": error, "timestamp": timestamp, "reprocess": reprocess, "key": key, "headers": headers}], quality_config)[0]

    def save_batch(self, records, quality_config=None):
        """一批一个 FULL 事务；缓存仅在本批内有效，回滚不会污染下一批状态。"""
        saved = []
        snapshots = {}
        dirty = set()
        touched = set()
        gates = None
        insert_record = self.db.cursor()
        with self.db:
            for record in records:
                topic, partition, offset = record["topic"], record["partition"], record["offset"]
                raw = record["raw"]
                event, error = record.get("event"), record.get("error")
                prepared = []
                for item in events_of(event) if event else []:
                    if item["message_type"] != "media_command" and any(name in item["payload"] for name in DYNAMIC_FIELDS):
                        if gates is None:
                            gates = quality_config or {}
                            check_quality_config(gates)
                        rejections = position_gate_rejections(item["payload"], item.get("event_time"), gates)
                        item = {**item, "quality": {**item.get("quality", {}), "position_gate_rejections": rejections}}
                    prepared.append(item)
                if event:
                    event = {**event, "events": prepared} if event.get("message_type") == "batch" else prepared[0]
                serialized = None if event is None else ENCODE_JSON(event)
                key = record.get("key")
                headers = record.get("headers")
                key_encoded = base64.b64encode(key).decode("ascii") if key is not None else None
                header_encoded = (None if headers is None else "[]" if not headers else ENCODE_JSON([
                    {"key": name, "value_base64": base64.b64encode(value).decode("ascii") if value is not None else None}
                    for name, value in headers]))
                cursor = insert_record.execute("""
                    INSERT OR IGNORE INTO records
                    (topic,partition_id,offset_id,received_at,kafka_timestamp_ms,raw_base64,status,error,normalized_json,raw_is_null,kafka_key_base64,kafka_headers_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (topic, partition, offset, utc_now(), record.get("timestamp"),
                      base64.b64encode(raw if raw is not None else b"").decode("ascii"),
                      "invalid" if error else "valid", error, serialized, raw is None, key_encoded, header_encoded))
                if cursor.rowcount == 0:
                    if not record.get("reprocess") or event is None:
                        saved.append(False)
                        continue
                    self.db.execute("""UPDATE records SET status='valid',error=NULL,normalized_json=?
                        WHERE topic=? AND partition_id=? AND offset_id=?""", (serialized, topic, partition, offset))
                saved.append(True)
                for item in prepared:
                    if item["message_type"] == "media_command":
                        continue  # 媒体控制只有事件留档，不创建或更新 AIS 船舶快照。
                    mmsi = item["mmsi"]
                    if mmsi not in snapshots:
                        self.db.execute("INSERT OR IGNORE INTO latest_ship(mmsi) VALUES (?)", (mmsi,))
                        static, dynamic = self.db.execute("SELECT static_json,dynamic_json FROM latest_ship WHERE mmsi=?", (mmsi,)).fetchone()
                        snapshots[mmsi] = {"static": json.loads(static) if static else None,
                                           "dynamic": json.loads(dynamic) if dynamic else None}
                    for kind, names in (("static", STATIC_FIELDS), ("dynamic", DYNAMIC_FIELDS)):
                        if kind == "dynamic" and item.get("quality", {}).get("position_gate_rejections"):
                            continue
                        if not any(name in item["payload"] for name in names):
                            continue
                        snapshots[mmsi][kind] = merge_snapshot(snapshots[mmsi][kind], item, kind, names, finalize=False)
                        touched.add((mmsi, kind))
                        dirty.add(mmsi)
            for mmsi, kind in touched:
                finish_snapshot(snapshots[mmsi][kind])
            updates = []
            for mmsi in dirty:
                static, dynamic = snapshots[mmsi]["static"], snapshots[mmsi]["dynamic"]
                updates.append((static.get("event_time") if static else None,
                    ENCODE_JSON(static) if static else None,
                    dynamic.get("event_time") if dynamic else None,
                    ENCODE_JSON(dynamic) if dynamic else None, mmsi))
            self.db.executemany("UPDATE latest_ship SET static_time=?,static_json=?,dynamic_time=?,dynamic_json=? WHERE mmsi=?", updates)
        return saved

    def invalid_records(self):
        return self.db.execute("""SELECT topic,partition_id,offset_id,raw_base64,kafka_timestamp_ms,raw_is_null
            FROM records WHERE status='invalid' ORDER BY topic,partition_id,offset_id""").fetchall()

    def message_records(self, limit=None):
        """导出原始消息位置和规范化事件；不输出可能含设备凭证的原文。"""
        sql = """SELECT topic,partition_id,offset_id,received_at,kafka_timestamp_ms,status,error,normalized_json,kafka_key_base64,kafka_headers_json
                 FROM records ORDER BY received_at,topic,partition_id,offset_id"""
        if limit is not None:
            sql = sql.replace("ORDER BY received_at,topic,partition_id,offset_id", "ORDER BY received_at DESC,topic,partition_id,offset_id") + " LIMIT ?"
        rows = self.db.execute(sql, (limit,) if limit is not None else ())
        return [{"schema_version": RECORD_SCHEMA, "record_id": record_id(t,p,o),
                 "topic": t, "partition": p, "offset": o, "received_at": received,
                 "kafka_timestamp_ms": timestamp, "status": status, "error": error,
                 "message": json.loads(normalized) if normalized else None,
                 "kafka_metadata_recorded": headers is not None, "kafka_key_base64": key,
                 "kafka_headers": json.loads(headers) if headers is not None else None}
                for t, p, o, received, timestamp, status, error, normalized, key, headers in rows]

    def quality_report(self):
        return quality_report(self.db)

    def summary(self, quality_config=None):
        counts = dict(self.db.execute("SELECT status, COUNT(*) FROM records GROUP BY status"))
        ships = []
        for mmsi, dynamic, static in self.db.execute("SELECT mmsi,dynamic_json,static_json FROM latest_ship ORDER BY mmsi"):
            dynamic = json.loads(dynamic) if dynamic else None
            static = json.loads(static) if static else None
            fields = dict.fromkeys(BUSINESS_FIELDS)
            times = {}
            sources = {}
            for snapshot in (static, dynamic):
                if snapshot:
                    fields.update({name: value for name, value in snapshot["payload"].items() if name in BUSINESS_FIELDS})
                    times.update({name: value for name, value in field_times(snapshot).items() if name in BUSINESS_FIELDS})
                    sources.update(snapshot.get("field_sources", {}))
            lon_time, lat_time = times.get("longitude"), times.get("latitude")
            position_time = lon_time if lon_time and lon_time == lat_time else None
            ais = {"mmsi": mmsi, **fields, "position_time": position_time, "field_times": times,
                   "quality": assess(fields, position_time, quality_config)}
            if sources:
                ais["field_sources"] = sources
                coordinate_systems = {sources.get(name, {}).get("coordinate_system", "WGS84")
                                      for name in ("longitude", "latitude") if fields[name] is not None}
                ais["coordinate_system"] = (next(iter(coordinate_systems)) if len(coordinate_systems) == 1 else "unknown")
                if ais["coordinate_system"] != "WGS84":
                    ais["quality"]["rejection_reasons"].append("unconfirmed_coordinate_system")
                    ais["quality"].update(position_time_candidate=False, ready_for_matching=False)
            ships.append({"mmsi": mmsi, "ais": ais, "dynamic": dynamic, "static": static})
        errors = [{"topic": t, "partition": p, "offset": o, "error": e} for t,p,o,e in self.db.execute(
            "SELECT topic,partition_id,offset_id,error FROM records WHERE status='invalid' ORDER BY received_at DESC LIMIT 10")]
        return {"counts": counts, "ships": ships, "recent_errors": errors}

    def close(self):
        self.db.close()

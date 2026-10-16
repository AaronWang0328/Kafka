"""补充包逐船 AIS 导出；保留来源时间口径，不声称还原现网 Kafka value。"""
from datetime import datetime, timedelta, timezone
import math
import re

from .model import BUSINESS_FIELDS, InvalidMessage

TOPICS = ("ais_parsing_ori", "REALTIME_TOPIC")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
RECV_ZONE = timezone(timedelta(hours=8))
MMSI_PATTERN = re.compile(r"[0-9]{9}")
RECV_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}")


def fail(field):
    raise InvalidMessage(f"ct_ais: invalid_{field}")


def timestamp(value, unit):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        fail("last_updated")
    scaled = value * (1000 if unit == "seconds" else 1)
    if not 0 <= scaled <= 253402300799999:
        fail("last_updated")
    return math.floor(scaled)


def iso(milliseconds):
    return (EPOCH + timedelta(milliseconds=milliseconds)).isoformat(timespec="milliseconds")


def normalize_ais(data, config):
    if not isinstance(data, dict):
        fail("object")
    topic = data.get("topic")
    if topic not in TOPICS or (config.get("source_topic") and topic != config["source_topic"]):
        fail("topic")
    csv = data.get("format") == "ct_ais_track_csv"
    row = data.get("csv_row") if csv else data
    if not isinstance(row, dict) or row.get("topic") != topic:
        fail("object")
    mmsi = row.get("mmsi")
    if type(mmsi) is int:
        mmsi = str(mmsi)
    if not isinstance(mmsi, str) or not MMSI_PATTERN.fullmatch(mmsi):
        fail("mmsi")
    recv = row.get("recv")
    if not isinstance(recv, str) or not RECV_PATTERN.fullmatch(recv):
        fail("recv")
    try:
        received = datetime.fromisoformat(recv).replace(tzinfo=RECV_ZONE)
        delta = received.astimezone(timezone.utc) - EPOCH
        recv_ms = delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000
    except ValueError:
        fail("recv")
    def numeric(key, low, high=None, optional=False):
        value = row.get(key)
        if value is None and optional:
            return None
        if csv:
            try:
                value = float(value)
            except (ValueError, TypeError):
                fail(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < low or (high is not None and value > high):
            fail(key)
        return float(value)
    issues = []
    name = row.get("name")
    if name is not None and not isinstance(name, str):
        fail("name")
    name = name.strip() if name else None
    if not name:
        name = None
        issues.append("unknown_ship_name")
    length = numeric("ship_length", 0, optional=True)
    width = numeric("ship_width", 0, optional=True)
    if not length:
        length = None
        issues.append("unknown_length_m")
    if not width:
        width = None
        issues.append("unknown_width_m")
    course = numeric("course_deg" if csv else "course", 0, optional=True)
    if course is None or course >= 360:
        course = None
        issues.append("unavailable_course")
    heading = None if csv else numeric("heading", 0, optional=True)
    if heading is None or heading >= 360:
        heading = None
        issues.append("unavailable_heading")
    updated = None if csv else row.get("last_updated")
    unit = "seconds" if topic == "ais_parsing_ori" else "milliseconds"
    updated_ms = timestamp(updated, unit) if updated is not None else None
    updated_time = iso(updated_ms) if updated_ms is not None else None
    received_time = iso(recv_ms)
    event_time = updated_time if topic == "ais_parsing_ori" else None
    if event_time is None:
        issues.append("unconfirmed_position_time")
    coordinate = config.get("coordinate_system", "unknown")
    if coordinate == "unknown":
        issues.append("unconfirmed_coordinate_system")
    fields = {"ship_name": name, "length_m": length, "width_m": width,
              "longitude": numeric("lon", -180, 180), "latitude": numeric("lat", -90, 90),
              "sog_knots": numeric("speed_kn" if csv else "speed", 0, optional=True), "cog_deg": course}
    source = {"kind": "ais_track_csv" if csv else "ais_export", "topic": topic,
              "time_basis": "receive_only" if csv else "ais_last_updated" if topic == "ais_parsing_ori" else "platform_createDate",
              "received_time": received_time, "observation_time": updated_time if updated_ms is not None else received_time,
              "last_updated_time": updated_time,
              "last_updated_unit": None if csv else unit, "coordinate_system": coordinate,
              "reported_update_age_seconds": (recv_ms - updated_ms) / 1000 if updated_ms is not None else None,
              "heading_deg": heading,
              "raw_navigation": {key: row[key] for key in ("course", "heading", "last_updated") if key in row},
              "export_format": "csv_track_projection" if csv else "supplement_vessel_projection"}
    if csv:
        for key in ("source_file", "source_line"):
            if key in data:
                source[key] = data[key]
    return {"mmsi": mmsi, "event_time": event_time, "message_type": "combined", "payload": fields,
            "source": source, "quality": {"missing_fields": [key for key in BUSINESS_FIELDS if fields.get(key) is None],
                                           "source_time_known": event_time is not None, "issues": issues}}

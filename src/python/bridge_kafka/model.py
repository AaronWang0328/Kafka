"""HTML 1.2/4.2 的 AIS 业务字段与可配置 JSON 报文适配。"""
from copy import deepcopy
from datetime import datetime, timezone
import json
import math
import re

STATIC_FIELDS = ("ship_name", "length_m", "width_m", "moulded_depth_m")
DYNAMIC_FIELDS = ("longitude", "latitude", "sog_knots", "cog_deg")
BUSINESS_FIELDS = STATIC_FIELDS + DYNAMIC_FIELDS
MISSING = object()
CAPTURE_TOPICS = {"ct_alarm": "ABNORMAL_TOPIC", "ct_down_pic": "down_pic", "ct_down_pic_plus": "down_pic_plus"}


class InvalidMessage(ValueError):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def parse_time(value, timestamp_format="iso8601"):
    try:
        if timestamp_format in ("unix_seconds", "unix_milliseconds"):
            numeric = number(value, "event_time", 0)
            if timestamp_format == "unix_milliseconds":
                numeric /= 1000
            result = datetime.fromtimestamp(numeric, timezone.utc)
        elif timestamp_format == "iso8601":
            if not isinstance(value, str):
                raise InvalidMessage("event_time 必须是带时区的 ISO 8601 字符串")
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if result.tzinfo is None or result.utcoffset() is None:
                raise InvalidMessage("采集时间必须带时区，不猜测现场时区")
        else:
            raise InvalidMessage("不支持的 timestamp_format")
    except (ValueError, OverflowError, OSError) as exc:
        raise InvalidMessage(f"event_time 无效：{exc}") from exc
    return result.astimezone(timezone.utc).isoformat(timespec="milliseconds")


def number(value, name, low, high=None, exclusive_high=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidMessage(f"{name} 必须是数值")
    if not math.isfinite(value) or value < low:
        raise InvalidMessage(f"{name} 超出范围")
    if high is not None and (value >= high if exclusive_high else value > high):
        raise InvalidMessage(f"{name} 超出范围")
    return float(value)


def check_input_config(config):
    profile = config.get("profile", "standard")
    if profile not in ("standard", "ct_ais", *CAPTURE_TOPICS):
        raise ValueError("input.profile 必须是 standard、ct_ais、ct_alarm、ct_down_pic 或 ct_down_pic_plus")
    choices = {
        "speed_unit": ("knots", "mps", "kmh", "unknown") if profile == "ct_alarm" else ("knots", "mps", "kmh"),
        "dimension_unit": ("m", "cm"),
        "course_unit": ("degrees", "radians"),
        "coordinate_system": ("WGS84", "unknown") if profile in ("ct_alarm", "ct_ais") else ("WGS84",),
        "timestamp_format": ("iso8601", "unix_seconds", "unix_milliseconds"),
    }
    for key, supported in choices.items():
        if key in config and config[key] not in supported:
            raise ValueError(f"input.{key} 不支持 {config[key]!r}；需按真实样本补充适配")
    mapping = config.get("field_map", {})
    if not isinstance(mapping, dict) or any(not isinstance(path, str) or not path for path in mapping.values()):
        raise ValueError("input.field_map 必须是业务字段到 JSON 点路径的映射")
    unknown = set(mapping) - {"mmsi", "event_time", *BUSINESS_FIELDS}
    if unknown:
        raise ValueError(f"未知业务字段映射：{sorted(unknown)}")
    if mapping and "mmsi" not in mapping:
        raise ValueError("自定义字段映射必须指定 mmsi")
    if (profile in CAPTURE_TOPICS or profile == "ct_ais") and mapping:
        raise ValueError("现场协议配置使用已知字段映射；自定义 field_map 请使用 standard")
    if "source_topic" in config and (profile != "ct_ais" or config["source_topic"] not in ("ais_parsing_ori", "REALTIME_TOPIC")):
        raise ValueError("input.source_topic 仅用于 ct_ais 的两个已确认来源")
    if profile == "ct_ais" and any(config.get(k, v) != v for k,v in
            (("speed_unit", "knots"), ("dimension_unit", "m"), ("course_unit", "degrees"))):
        raise ValueError("ct_ais 的速度为节、长宽为米、航向为度，不能重复换算")
    if "alarm_angle_is_cog" in config and type(config["alarm_angle_is_cog"]) is not bool:
        raise ValueError("input.alarm_angle_is_cog 必须是布尔值")
    if profile == "ct_alarm" and (config.get("dimension_unit", "m") != "m" or
                                  config.get("course_unit", "degrees") != "degrees"):
        raise ValueError("ct_alarm 的尺寸为米，shipAngle 原值为度；请勿重复换算")


def at_path(data, path):
    value = data
    for name in path.split("."):
        if not isinstance(value, dict) or name not in value:
            return MISSING
        value = value[name]
    return value


def decode_json(raw):
    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("JSON 数值必须有限")
        return number

    def invalid_constant(value):
        raise ValueError(f"JSON 不允许 {value}")

    try:
        return json.loads(raw, parse_float=finite_float, parse_constant=invalid_constant)
    except (ValueError, UnicodeError, TypeError) as exc:
        raise InvalidMessage("消息不是有效 UTF-8 JSON") from exc


def events_of(message):
    """兼容旧单船结果；多船消息仍只对应一个 Kafka offset。"""
    return message["events"] if message.get("message_type") == "batch" else [message]


def unwrap_capture(raw, input_config):
    """解开已确认的消费留档；原 offset 仅为来源证据，不作为目标消费进度。"""
    expected = CAPTURE_TOPICS.get(input_config.get("profile"))
    if expected:
        data = decode_json(raw)
        if isinstance(data, dict) and "value" in data and "topic" in data:
            if data["topic"] != expected:
                raise InvalidMessage(f"消费留档 Topic 与当前协议不符，期望 {expected}")
            if not isinstance(data["value"], str):
                raise InvalidMessage("消费留档的 value 必须是 JSON 字符串")
            for name in ("partition", "offset", "kafka_ts"):
                value = data.get(name)
                if value is not None and (type(value) is not int or value < 0):
                    raise InvalidMessage(f"消费留档 {name} 必须是非负整数或 null")
            if "recv" in data and not isinstance(data["recv"], str):
                raise InvalidMessage("消费留档 recv 必须是字符串")
            metadata = {name: data[name] for name in ("topic", "partition", "offset", "kafka_ts", "recv") if name in data}
            try:
                return data["value"].encode("utf-8"), metadata
            except UnicodeError as exc:
                raise InvalidMessage("消费留档 value 不是有效 UTF-8") from exc
    return raw, None


def capture_payload(raw, input_config):
    """回放原始 value 字节，不把留档外层包装发送到目标 Topic。"""
    return unwrap_capture(raw, input_config)[0]


def normalize(raw, input_config=None):
    config = input_config or {}
    check_input_config(config)
    return _normalize(raw, config)


def make_normalizer(input_config=None):
    """为一次导入/消费固定配置；拷贝后校验一次，不缓存外部可变字典。"""
    config = deepcopy(input_config or {})
    check_input_config(config)
    return lambda raw: _normalize(raw, config)


def _normalize(raw, config):
    payload, capture = unwrap_capture(raw, config)
    data = decode_json(payload)
    if config.get("profile") == "ct_ais":
        from .continuous import normalize_ais
        return normalize_ais(data, config)
    if config.get("profile") in ("ct_down_pic", "ct_down_pic_plus"):
        from .site_protocols import normalize_media_command
        result = normalize_media_command(data, config["profile"])
        if capture is not None:
            result["capture"] = capture
        return result
    items = data if isinstance(data, list) else [data]
    if not items or len(items) > 10000:
        raise InvalidMessage("JSON 数组必须含 1–10000 个船舶对象")
    events = []
    for index, item in enumerate(items):
        try:
            events.append(normalize_object(item, config))
        except InvalidMessage as exc:
            raise InvalidMessage(f"对象[{index}]：{exc}") from exc
    result = events[0] if len(events) == 1 else {"message_type": "batch", "events": events}
    if capture is not None:
        result["capture"] = capture
    return result


def normalize_object(data, config):
    if not isinstance(data, dict):
        raise InvalidMessage("船舶消息必须是 JSON 对象")
    source = None
    alarm = None
    if config.get("profile") == "ct_alarm":
        from .site_protocols import alarm_details
        alarm = alarm_details(data)
        # 告警时刻与船舶采集时间不同；未确认单位/语义的字段只作原值保留。
        observation_time = data.get("warnDate")
        if observation_time is not None:
            observation_time = parse_time(observation_time, "unix_milliseconds")
        source = {"kind": "alarm_snapshot", "observation_time": observation_time,
                  "time_basis": "alarm", "coordinate_system": config.get("coordinate_system", "unknown"),
                  "event_id": data.get("eventId"), "warn_type": data.get("warnType"),
                  "raw_navigation": {key: data[key] for key in
                      ("shipSpeed", "shipAngle", "shipDirection", "shipHeight") if key in data}}
        alarm_mapping = {"mmsi": "mmsi", "ship_name": "name", "length_m": "shipLength",
                         "width_m": "shipWidth", "longitude": "longitude", "latitude": "latitude"}
        if config.get("speed_unit", "unknown") != "unknown":
            alarm_mapping["sog_knots"] = "shipSpeed"
        if config.get("alarm_angle_is_cog", False):
            alarm_mapping["cog_deg"] = "shipAngle"
        values = {name: data[key] for name, key in alarm_mapping.items() if key in data}
        for name in ("length_m", "width_m"):
            if type(values.get(name)) in (int, float) and values[name] == 0:
                values[name] = None
        # 继续复用标准校验；绝不将 shipHeight 映射成型深。
        data = values
    mapping = config.get("field_map")
    if mapping:
        values = {name: value for name, path in mapping.items() if (value := at_path(data, path)) is not MISSING}
    else:
        # 接受直接业务字段；兼容早期本地 payload 包装，不要求上游有自定义元数据。
        payload = data.get("payload", data)
        if not isinstance(payload, dict):
            raise InvalidMessage("payload 必须是对象")
        values = {name: payload[name] for name in BUSINESS_FIELDS if name in payload}
        for name in ("mmsi", "event_time"):
            if name in data:
                values[name] = data[name]
    mmsi = values.get("mmsi")
    if type(mmsi) is int:
        mmsi = str(mmsi)
    if not isinstance(mmsi, str) or not re.fullmatch(r"[0-9]{9}", mmsi):
        raise InvalidMessage("mmsi 必须是 9 位数字字符串或 9 位整数")
    static = any(name in values for name in STATIC_FIELDS)
    dynamic = any(name in values for name in DYNAMIC_FIELDS)
    if not static and not dynamic:
        raise InvalidMessage("未找到 HTML 要求的 AIS 业务字段，请检查报文或配置字段映射")
    kind = "combined" if static and dynamic else "static" if static else "dynamic"
    event_time = values.get("event_time")
    if event_time is not None:
        event_time = parse_time(event_time, config.get("timestamp_format", "iso8601"))
    clean = {}
    for name in BUSINESS_FIELDS:
        if name not in values:
            continue  # 未提供字段不能被当成 null 更新，保留先前的登记资料。
        value = values[name]
        if value is None:
            clean[name] = None
        elif name == "ship_name":
            if not isinstance(value, str) or not value.strip():
                raise InvalidMessage("ship_name 必须为非空字符串或 null")
            clean[name] = value.strip()
        elif name in ("length_m", "width_m", "moulded_depth_m"):
            clean[name] = number(value, name, 0)
            if clean[name] == 0:
                raise InvalidMessage(f"{name} 必须大于 0；未知值由适配器转为 null")
            if config.get("dimension_unit", "m") == "cm":
                clean[name] /= 100
        elif name == "longitude":
            clean[name] = number(value, name, -180, 180)
        elif name == "latitude":
            clean[name] = number(value, name, -90, 90)
        elif name == "sog_knots":
            clean[name] = number(value, name, 0)
            if config.get("speed_unit", "knots") == "mps":
                clean[name] *= 3600 / 1852
            elif config.get("speed_unit", "knots") == "kmh":
                clean[name] /= 1.852
        elif name == "cog_deg":
            if config.get("course_unit", "degrees") == "radians":
                value = number(value, name, 0, 2 * math.pi, True) * 180 / math.pi
            clean[name] = number(value, name, 0, 360, True)
    required = BUSINESS_FIELDS if kind == "combined" else STATIC_FIELDS if kind == "static" else DYNAMIC_FIELDS
    missing = [name for name in required if clean.get(name) is None]
    result = {"mmsi": mmsi, "event_time": event_time, "message_type": kind, "payload": clean,
              "quality": {"missing_fields": missing, "source_time_known": event_time is not None}}
    if source is not None:
        result["source"] = source
        result["alarm"] = alarm
    return result

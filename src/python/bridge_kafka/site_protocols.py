"""第二批包提供的告警及两种媒体控制协议；只解析，不执行抓图录像。"""
from pathlib import PurePosixPath
from urllib.parse import urlsplit, urlunsplit

from .model import InvalidMessage, number, parse_time


def required_text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise InvalidMessage(f"{name} 必须是非空字符串")
    return value


def media_path(value, name):
    value = required_text(value, name)
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or "\\" in value or any(ord(c) < 32 for c in value):
        raise InvalidMessage(f"{name} 必须是有效的相对媒体路径")
    return value


def public_uri(value, name, schemes):
    """普通校验/导出只保留去除用户凭证的地址；完整原文仍在留档中。"""
    required_text(value, name)
    try:
        uri = urlsplit(value)
        host = uri.hostname
        port = uri.port
        if uri.scheme not in schemes or not host or any(c.isspace() for c in value):
            raise ValueError()
        hostname = f"[{host}]" if ":" in host else host
        netloc = f"{hostname}:{port}" if port is not None else hostname
        return urlunsplit((uri.scheme, netloc, uri.path, uri.query, uri.fragment)), uri.username is not None or uri.password is not None
    except ValueError as exc:
        raise InvalidMessage(f"{name} 不是有效的媒体地址") from exc


def alarm_details(data):
    warn_type = data.get("warnType")
    if type(warn_type) is not int or not 1 <= warn_type <= 8:
        raise InvalidMessage("warnType 必须是 1–8 的整数")
    if data.get("warnDate") is None:
        raise InvalidMessage("告警必须提供 warnDate 毫秒时间戳")
    result = {"warn_type": warn_type, "warn_time": parse_time(data["warnDate"], "unix_milliseconds")}
    for source, target in (("eventId", "event_id"), ("warnInfo", "info"), ("warnGrade", "grade"), ("warnAction", "action")):
        if source in data:
            result[target] = required_text(data[source], source)
    for source, target in (("warnImageUrl", "image_url"), ("warnVideoUrl", "video_url")):
        if data.get(source):
            result[target], _ = public_uri(data[source], source, ("http", "https"))
    return result


def normalize_media_command(data, profile):
    protocol = "down_pic" if profile == "ct_down_pic" else "down_pic_plus"
    if protocol == "down_pic":
        if not isinstance(data, str):
            raise InvalidMessage("down_pic 的 JSON value 必须是字符串")
        parts = data.split(",")
        if len(parts) != 4:
            raise InvalidMessage("down_pic 字符串必须包含经度、纬度、图片路径、视频路径四段")
        try:
            lon, lat = float(parts[0]), float(parts[1])
        except ValueError as exc:
            raise InvalidMessage("down_pic 经纬度必须是数值字符串") from exc
        payload = {"longitude": number(lon, "longitude", -180, 180),
                   "latitude": number(lat, "latitude", -90, 90),
                   "coordinate_system": "unknown", "picture_path": media_path(parts[2], "picture_path"),
                   "video_path": media_path(parts[3], "video_path")}
    else:
        if not isinstance(data, list) or len(data) != 4:
            raise InvalidMessage("down_pic_plus 的 JSON value 必须是四元素数组")
        uri, credentials = public_uri(data[0], "camera_uri", ("rtsp", "rtsps"))
        if type(data[3]) is not bool:
            raise InvalidMessage("down_pic_plus 第四项必须是布尔值，其业务含义待确认")
        payload = {"camera_uri": uri, "credentials_present": credentials,
                   "picture_path": media_path(data[1], "picture_path"),
                   "video_path": media_path(data[2], "video_path"), "option": data[3],
                   "option_semantics": "unconfirmed"}
    return {"message_type": "media_command", "protocol": protocol, "event_time": None,
            "payload": payload, "quality": {"source_time_known": False}}

"""HTML 4.3/4.6：为位置+时间关联提供 AIS 侧门限，不执行雷达匹配。"""
from datetime import datetime, timezone
import math

from .model import BUSINESS_FIELDS


def check_quality_config(config):
    for key in ("max_position_age_seconds", "max_future_skew_seconds"):
        value = config.get(key, 0)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"quality.{key} 必须是非负有限数值；0 表示未配置")
    bounds = config.get("bounds", {})
    if bounds:
        for key in ("min_longitude", "max_longitude", "min_latitude", "max_latitude"):
            value = bounds.get(key)
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError("quality.bounds 必须完整指定四个经纬度边界")
        if not (-180 <= bounds["min_longitude"] <= bounds["max_longitude"] <= 180 and
                -90 <= bounds["min_latitude"] <= bounds["max_latitude"] <= 90):
            raise ValueError("quality.bounds 范围或顺序错误")


def position_gate_rejections(fields, position_time, config, now=None):
    """仅供已校验配置的入库路径使用；完整质量报告仍由 assess 生成。"""
    reasons = []
    lon, lat = fields.get("longitude"), fields.get("latitude")
    bounds = config.get("bounds", {})
    if bounds and lon is not None and lat is not None:
        if not (bounds["min_longitude"] <= lon <= bounds["max_longitude"] and
                bounds["min_latitude"] <= lat <= bounds["max_latitude"]):
            reasons.append("outside_configured_area")
    if position_time:
        age = ((now or datetime.now(timezone.utc)) - datetime.fromisoformat(position_time)).total_seconds()
        if config.get("max_position_age_seconds", 0) and age > config["max_position_age_seconds"]:
            reasons.append("stale_position")
        if config.get("max_future_skew_seconds", 0) and age < -config["max_future_skew_seconds"]:
            reasons.append("future_position_time")
    return reasons


def assess(fields, position_time, config=None, now=None):
    config = config or {}
    check_quality_config(config)
    missing = [name for name in BUSINESS_FIELDS if fields.get(name) is None]
    reasons = []
    lon, lat = fields.get("longitude"), fields.get("latitude")
    if lon is None or lat is None:
        reasons.append("missing_position")
    if not position_time:
        reasons.append("missing_or_inconsistent_position_time")
    bounds = config.get("bounds", {})
    if bounds and lon is not None and lat is not None:
        if not (bounds["min_longitude"] <= lon <= bounds["max_longitude"] and
                bounds["min_latitude"] <= lat <= bounds["max_latitude"]):
            reasons.append("outside_configured_area")
    age = None
    if position_time:
        current = now or datetime.now(timezone.utc)
        age = (current - datetime.fromisoformat(position_time)).total_seconds()
        if config.get("max_position_age_seconds", 0) and age > config["max_position_age_seconds"]:
            reasons.append("stale_position")
        if config.get("max_future_skew_seconds", 0) and age < -config["max_future_skew_seconds"]:
            reasons.append("future_position_time")
    gates_configured = bool(bounds and config.get("max_position_age_seconds", 0)
                            and config.get("max_future_skew_seconds", 0))
    return {"missing_fields": missing, "document_fields_complete": not missing,
            "position_age_seconds": age, "rejection_reasons": reasons,
            "position_time_candidate": not reasons,
            "project_gates_configured": gates_configured,
            "ready_for_matching": not reasons and gates_configured}

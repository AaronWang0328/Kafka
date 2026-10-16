import os
from pathlib import Path
import tomllib

from .model import check_input_config
from .quality import check_quality_config

ROOT = Path(__file__).resolve().parents[3]


def load_settings(path=None):
    path = Path(path) if path else ROOT / "configs/ais-origin.toml"
    with path.open("rb") as handle:
        settings = tomllib.load(handle)
    kafka = settings["kafka"]
    topic = os.getenv("AIS_TOPIC", kafka.get("topic", ""))
    if not isinstance(topic, str) or not topic.strip():
        raise ValueError("必须提供已确认的 Kafka Topic；普通 AIS Topic 尚未知时不能猜测名称")
    conf = {
        "bootstrap.servers": os.getenv("AIS_BOOTSTRAP_SERVERS", kafka["bootstrap_servers"]),
        "security.protocol": os.getenv("AIS_SECURITY_PROTOCOL", kafka["security_protocol"]),
    }
    # 凭证由环境变量传入，不写入源代码或输出到日志。
    for env_name, option in (
        ("AIS_SASL_MECHANISM", "sasl.mechanism"),
        ("AIS_SASL_USERNAME", "sasl.username"),
        ("AIS_SASL_PASSWORD", "sasl.password"),
        ("AIS_SSL_CA_LOCATION", "ssl.ca.location"),
    ):
        if os.getenv(env_name):
            conf[option] = os.environ[env_name]
    database = Path(settings["storage"]["database"])
    input_config = settings.get("input", {})
    quality_config = settings.get("quality", {})
    check_input_config(input_config)
    check_quality_config(quality_config)
    processing = {"batch_size": 2048, "batch_max_wait_ms": 50, **settings.get("processing", {})}
    for name in ("batch_size", "batch_max_wait_ms"):
        if type(processing[name]) is not int or processing[name] < 1:
            raise ValueError(f"processing.{name} 必须是正整数")
    if processing["batch_size"] > 10000:
        raise ValueError("processing.batch_size 不能超过 10000，以限制内存和单次处理时间")
    return {
        "client": conf,
        "topic": topic,
        "group": os.getenv("AIS_GROUP_ID", kafka["group_id"]),
        "reset": kafka.get("auto_offset_reset", "earliest"),
        "database": database if database.is_absolute() else ROOT / database,
        "input": input_config,
        "quality": quality_config,
        "processing": processing,
    }

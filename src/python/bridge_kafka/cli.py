import argparse
import base64
import json
import logging
from pathlib import Path
import time

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .batching import BatchWriter
from .model import InvalidMessage, capture_payload, decode_json, events_of, make_normalizer
from .offline import import_file, jsonl_records
from .settings import load_settings
from .store import Store

LOG = logging.getLogger("ais")


def init_topic(settings):
    admin = AdminClient(settings["client"])
    topic = settings["topic"]
    metadata = admin.list_topics(timeout=10)
    if topic in metadata.topics and not metadata.topics[topic].error:
        LOG.info("Topic 已存在：%s", topic)
        return
    future = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1,
        config={"retention.ms": "604800000", "cleanup.policy": "delete"})], request_timeout=10)[topic]
    future.result(timeout=15)
    LOG.info("已创建 Topic：%s（1 分区、1 副本、保留 7 天）", topic)


def produce(settings, args):
    if not args.file:
        raise RuntimeError("必须通过 --file 指定已有数据文件；模拟数据生成入口已移除")
    # 先校验整份样本，避免在后半段格式有误时已发送前半段。
    allow_invalid = getattr(args, "allow_invalid", False)
    invalid = 0
    normalize = make_normalizer(settings["input"])
    for _, raw in jsonl_records(args.file):
        if settings["input"].get("profile") == "ct_ais":
            # 即使显式回放异常原文，也不能把另一个来源发送到错误通道。
            try:
                row = decode_json(raw)
            except InvalidMessage:
                row = None
            if isinstance(row, dict) and row.get("topic") != settings["topic"]:
                raise InvalidMessage("ct_ais: invalid_topic")
        try:
            normalize(raw)
        except InvalidMessage:
            if not allow_invalid:
                raise
            invalid += 1
    producer = Producer({**settings["client"], "enable.idempotence": True, "acks": "all",
                         "delivery.timeout.ms": 15000, "linger.ms": 5, "batch.size": 65536})
    failures = []
    sent = 0

    def delivered(err, message):
        if err:
            failures.append(str(err))
        else:
            LOG.debug("发送成功 topic=%s partition=%d offset=%d", message.topic(), message.partition(), message.offset())

    events = (capture_payload(raw, settings["input"]) for _, raw in jsonl_records(args.file))
    try:
        for encoded in events:
            try:
                event = normalize(encoded)
                items = events_of(event)
            except InvalidMessage:
                if not allow_invalid:
                    raise
                items = []
            mmsis = {item.get("mmsi") for item in items}
            key = next(iter(mmsis)).encode("ascii") if len(mmsis) == 1 and None not in mmsis else None
            while True:
                try:
                    producer.produce(settings["topic"], key=key, value=encoded, on_delivery=delivered)
                    break
                except BufferError:
                    producer.poll(0.1)
            if sent % 512 == 0:
                producer.poll(0)
            sent += 1
            if args.interval:
                time.sleep(args.interval)
    finally:
        remaining = producer.flush(20)
    if remaining or failures:
        raise RuntimeError(f"消息发送失败或未确认：remaining={remaining}, errors={failures}")
    LOG.info("发送完成：%d 条消息，其中显式回放的无效原文 %d 条", sent, invalid)


def consume(settings, args):
    batch_size = args.batch_size or settings["processing"]["batch_size"]
    max_wait = (args.batch_max_wait_ms or settings["processing"]["batch_max_wait_ms"]) / 1000
    store = Store(settings["database"])
    consumer, writer = None, None
    count = 0
    active_started = None
    closing = False
    last_message = time.monotonic()
    try:
        consumer = Consumer({**settings["client"], "group.id": settings["group"],
            "auto.offset.reset": settings["reset"], "enable.auto.commit": False,
            "enable.auto.offset.store": False, "allow.auto.create.topics": False})
        writer = BatchWriter(store, consumer, settings, args.verbose_records)

        def revoked(client, partitions):
            if not closing:
                writer.flush()

        def lost(client, partitions):
            if not closing:
                # 已失去分区所有权：只留档，不能覆盖新消费者的进度。
                writer.flush(commit=False)

        consumer.subscribe([settings["topic"]], on_revoke=revoked, on_lost=lost)
        LOG.info("开始接收 topic=%s group=%s database=%s batch_size=%d batch_max_wait_ms=%d",
                 settings["topic"], settings["group"], settings["database"], batch_size, int(max_wait * 1000))
        while args.max_messages == 0 or count < args.max_messages:
            remaining = batch_size - len(writer.pending)
            if args.max_messages:
                remaining = min(remaining, args.max_messages - count)
            wait = max(0, max_wait - (time.monotonic() - writer.started)) if writer.pending else max_wait
            if writer.pending:
                messages = consumer.consume(num_messages=remaining, timeout=wait)
            else:
                # 第一条先单独取出，计时从它到达开始，避免稀疏流量多等一次。
                first = consumer.poll(max_wait)
                messages = [first] if first is not None else []
            if messages and active_started is None:
                active_started = time.monotonic()
            for msg in messages:
                if msg.error():
                    raise KafkaException(msg.error())
                writer.add(msg)
                count += 1
                last_message = time.monotonic()
            if writer.pending and (len(writer.pending) >= batch_size or time.monotonic() - writer.started >= max_wait):
                writer.flush()
            if not messages and args.idle_timeout and time.monotonic() - last_message >= args.idle_timeout:
                LOG.info("达到空闲超时，退出；本次接收 %d 条", count)
                break
        writer.flush()
        print(json.dumps({"processed": count, "active_seconds": time.monotonic() - active_started
                          if active_started is not None else 0, "mode": "kafka-batch"}))
        if args.max_messages and count < args.max_messages:
            raise RuntimeError(f"只接收 {count} 条，未达到要求的 {args.max_messages} 条")
    except KeyboardInterrupt:
        if writer:
            writer.flush()
        raise
    finally:
        closing = True
        try:
            if consumer:
                consumer.close()
        finally:
            store.close()


def main():
    parser = argparse.ArgumentParser(description="AIS 接入与现场告警、媒体指令的独立校验和留档")
    parser.add_argument("--config", help="配置文件路径，默认 configs/ais-origin.toml")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init", help="创建本地 AIS Topic（正式集群请由管理员创建）")
    sender = commands.add_parser("produce", help="回放已有 JSONL 原文，不生成模拟数据")
    sender.add_argument("--interval", type=float, default=0.2, help="每条消息之间等待的秒数")
    sender.add_argument("--file", required=True, help="已有 JSONL 路径，按 input 字段映射校验后发送")
    sender.add_argument("--allow-invalid", action="store_true", help="显式回放原文件中的异常行，供接收端无损留档；不跳过或修造数据")
    receiver = commands.add_parser("consume", help="持续接收、校验、保存并提交读取进度")
    receiver.add_argument("--max-messages", type=int, default=0, help="0 表示持续运行")
    receiver.add_argument("--idle-timeout", type=float, default=0, help="0 表示不因空闲退出")
    receiver.add_argument("--batch-size", type=int, help="每批最大消息数，默认读取 processing.batch_size")
    receiver.add_argument("--batch-max-wait-ms", type=int, help="批次最大等待毫秒数，默认读取 processing.batch_max_wait_ms")
    receiver.add_argument("--verbose-records", action="store_true", help="逐条打印留档结果，适合入门联调")
    commands.add_parser("inspect", help="查看本地数据统计、每船最新状态和最近错误")
    validator = commands.add_parser("validate", help="按所选协议离线检查报文，不发送消息")
    validator.add_argument("--file", required=True, help="JSONL 样本或消费留档文件")
    importer = commands.add_parser("import-file", help="离线导入 JSONL 到留档库，不连接 Kafka")
    importer.add_argument("--file", required=True, help="JSONL 样本或消费留档文件")
    importer.add_argument("--source-id", help="离线来源标识；内容 SHA-256 自动加入去重命名空间")
    exporter = commands.add_parser("export", help="导出按 MMSI 汇总的 AIS 业务字段，供后续融合接口联调")
    exporter.add_argument("--file", default="var/ais_for_fusion.json", help="输出文件路径")
    history = commands.add_parser("export-records", help="导出告警/指令历史与来源位置，不输出原文中的凭证")
    history.add_argument("--file", required=True, help="事件记录 JSON 输出路径")
    commands.add_parser("quality", help="输出确定性质量统计与异常来源位置，不改变业务数据")
    commands.add_parser("reprocess", help="按当前字段映射重解析已留档的无效消息，不改动 Kafka 进度")
    args = parser.parse_args()
    for name in ("interval", "max_messages", "idle_timeout"):
        if hasattr(args, name) and getattr(args, name) < 0:
            parser.error(f"{name} 不能为负数")
    for name in ("batch_size", "batch_max_wait_ms"):
        value = getattr(args, name, None)
        if value is not None and (value <= 0 or (name == "batch_size" and value > 10000)):
            parser.error(f"{name} 必须为正整数，batch_size 上限为 10000")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        settings = load_settings(args.config)
        if args.command == "init":
            init_topic(settings)
        elif args.command == "produce":
            produce(settings, args)
        elif args.command == "consume":
            consume(settings, args)
        elif args.command == "validate":
            invalid = 0
            normalize = make_normalizer(settings["input"])
            for line_number, raw in jsonl_records(args.file):
                try:
                    message = normalize(raw)
                    result = {"line": line_number, "command" if message["message_type"] == "media_command" else "ais": message}
                except InvalidMessage as exc:
                    result = {"line": line_number, "error": str(exc)}
                    invalid += 1
                print(json.dumps(result, ensure_ascii=False, allow_nan=False))
            return 1 if invalid else 0
        elif args.command == "import-file":
            result = import_file(settings, args.file, args.source_id)
            print(json.dumps(result, ensure_ascii=False, allow_nan=False))
            return 1 if result["invalid"] else 0
        else:
            store = Store(settings["database"])
            try:
                if args.command == "quality":
                    print(json.dumps(store.quality_report(), ensure_ascii=False, indent=2, allow_nan=False))
                elif args.command == "reprocess":
                    normalize = make_normalizer(settings["input"])
                    recovered = 0
                    rows = store.invalid_records()
                    for topic, partition, offset, raw, timestamp, is_null in rows:
                        raw = None if is_null else base64.b64decode(raw)
                        try:
                            event = normalize(raw)
                        except InvalidMessage:
                            continue
                        store.save(topic, partition, offset, raw, event, timestamp=timestamp, reprocess=True,
                                   quality_config=settings["quality"])
                        recovered += 1
                    print(json.dumps({"examined": len(rows), "recovered": recovered}, ensure_ascii=False))
                elif args.command in ("export", "export-records"):
                    output = Path(args.file)
                    output.parent.mkdir(parents=True, exist_ok=True)
                    result = (store.message_records() if args.command == "export-records" else
                              [ship["ais"] for ship in store.summary(settings["quality"])["ships"]])
                    # 写完临时文件后原子替换，避免业务方读到半个 JSON。
                    temporary = output.with_name(output.name + ".tmp")
                    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")
                    temporary.replace(output)
                    LOG.info("已导出 %d 条%s：%s", len(result), "事件记录" if args.command == "export-records" else "船舶资料", output)
                else:
                    print(json.dumps({**store.summary(settings["quality"]), "recent_records": store.message_records(10)},
                                     ensure_ascii=False, indent=2))
            finally:
                store.close()
    except KeyboardInterrupt:
        LOG.info("已停止")
    except Exception as exc:
        LOG.error("运行失败：%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

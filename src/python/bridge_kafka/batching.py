"""有界批次：先把全部记录持久化，再提交各分区的下一读取位置。"""
import logging
import time

from confluent_kafka import KafkaException, TopicPartition

from .model import InvalidMessage, events_of, make_normalizer

LOG = logging.getLogger("ais")


class BatchWriter:
    def __init__(self, store, consumer, settings, verbose=False):
        self.store, self.consumer, self.settings = store, consumer, settings
        self.normalize = make_normalizer(settings["input"])
        self.verbose = verbose
        self.pending = []
        self.started = None
        self.failed = False

    def add(self, msg):
        event, error = None, None
        try:
            event = self.normalize(msg.value())
        except InvalidMessage as exc:
            error = str(exc)
        timestamp = msg.timestamp()[1]
        if not self.pending:
            self.started = time.monotonic()
        self.pending.append({"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
            "raw": msg.value(), "event": event, "error": error,
            "timestamp": timestamp if timestamp >= 0 else None, "key": msg.key(), "headers": msg.headers() or ()})

    def flush(self, commit=True):
        if not self.pending or self.failed:
            return
        try:
            inserted = self.store.save_batch(self.pending, self.settings["quality"])
            offsets = {}
            for record in self.pending:
                key = (record["topic"], record["partition"])
                offsets[key] = max(offsets.get(key, 0), record["offset"] + 1)
            if commit:
                result = self.consumer.commit(offsets=[TopicPartition(topic, partition, offset)
                    for (topic, partition), offset in offsets.items()], asynchronous=False)
                for partition in result or []:
                    if partition.error:
                        raise KafkaException(partition.error)
            if self.verbose:
                for record, saved in zip(self.pending, inserted):
                    LOG.info("%s partition=%d offset=%d subject=%s error=%s",
                        "已保存" if saved else "重读已去重", record["partition"], record["offset"],
                        ",".join(item.get("mmsi", item.get("protocol", "-")) for item in events_of(record["event"])) if record["event"] else "-", record["error"] or "-")
            else:
                LOG.info("批次完成 records=%d inserted=%d invalid=%d offsets_committed=%s",
                    len(self.pending), sum(inserted), sum(bool(row["error"]) for row in self.pending), commit)
            self.pending.clear()
            self.started = None
        except BaseException:
            # 提交失败退出；重启重读并按 topic/partition/offset 去重。
            self.failed = True
            raise

"""JSONL 离线导入：原文留档与 Kafka offset 使用独立命名空间。"""
import hashlib
from pathlib import Path

from .model import InvalidMessage, make_normalizer
from .store import Store


def jsonl_records(path):
    with open(path, "rb") as handle:
        for line_number, line in enumerate(handle, 1):
            raw = line.rstrip(b"\r\n")
            if line_number == 1:
                raw = raw.removeprefix(b"\xef\xbb\xbf")
            if raw.strip():
                yield line_number, raw


def import_file(settings, path, source_id=None):
    path = Path(path)
    with path.open("rb") as handle:
        digest = hashlib.file_digest(handle, "sha256").hexdigest()
    source = f"offline:{source_id or path.name}:{digest}"
    normalize = make_normalizer(settings["input"])
    pending = []
    total = invalid = inserted = 0
    store = Store(settings["database"])
    try:
        for line_number, raw in jsonl_records(path):
            event, error = None, None
            try:
                event = normalize(raw)
            except InvalidMessage as exc:
                error = str(exc)
                invalid += 1
            pending.append(dict(topic=source, partition=0, offset=line_number - 1,
                                raw=raw, event=event, error=error))
            total += 1
            if len(pending) >= settings["processing"]["batch_size"]:
                inserted += sum(store.save_batch(pending, settings["quality"]))
                pending.clear()
        inserted += sum(store.save_batch(pending, settings["quality"]))
        return {"mode": "offline", "source": source, "messages": total, "invalid": invalid,
                "inserted": inserted, "duplicates": total - inserted,
                "ships": len(store.summary(settings["quality"])["ships"])}
    finally:
        store.close()

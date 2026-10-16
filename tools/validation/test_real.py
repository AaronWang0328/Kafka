"""仅使用交付原件；不构造、修改或生成船舶数据。"""
import json
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest

from bridge_kafka.model import InvalidMessage, make_normalizer, normalize
from bridge_kafka.offline import import_file, jsonl_records
from bridge_kafka.settings import ROOT, load_settings
from bridge_kafka.store import Store
from bridge_kafka.supplement import PREPARED
from tools.common import AIS_CHANNELS, SITE_CHANNELS, SITE_SOURCE, BINARY
from tools.validation.compare import compare_databases


def inputs():
    for topic,config in AIS_CHANNELS:
        yield topic, config, PREPARED/(topic+'.jsonl')
    for config,name in SITE_CHANNELS:
        settings=load_settings(ROOT/config)
        yield settings['topic'], config, SITE_SOURCE/name


def real_prefix(source, target, limit=1000):
    with target.open('wb') as output:
        for i,(_,raw) in enumerate(jsonl_records(source)):
            if i == limit:
                break
            output.write(raw+b'\n')


class RealDeliveryTests(unittest.TestCase):
    def test_five_topic_configurations_and_original_sources_exist(self):
        for topic,config,source in inputs():
            self.assertEqual(load_settings(ROOT/config)['topic'],topic)
            self.assertTrue(source.is_file(),str(source))

    def test_stream_and_single_normalization_match_on_original_rows(self):
        for topic,config,source in inputs():
            settings=load_settings(ROOT/config);stream=make_normalizer(settings['input'])
            for i,(_,raw) in enumerate(jsonl_records(source)):
                if i == 1000:
                    break
                try:
                    expected=normalize(raw,settings['input'])
                except InvalidMessage as old:
                    with self.assertRaises(InvalidMessage) as new:
                        stream(raw)
                    self.assertEqual(str(old),str(new.exception))
                else:
                    self.assertEqual(expected,stream(raw))

    def test_actual_invalid_mmsi_is_rejected_without_repair(self):
        for topic,config in AIS_CHANNELS:
            settings=load_settings(ROOT/config)
            for _,raw in jsonl_records(PREPARED/(topic+'.jsonl')):
                mmsi=str(json.loads(raw)['mmsi'])
                if len(mmsi)!=9:
                    with self.assertRaisesRegex(InvalidMessage,'invalid_mmsi'):
                        normalize(raw,settings['input'])
                    break
            else:
                self.fail('真实交付中的异常标识未找到')

    def test_python_and_rust_validate_original_site_captures_identically(self):
        self.assertTrue(BINARY.is_file(),'先执行 build-rust')
        for config,name in SITE_CHANNELS:
            settings=load_settings(ROOT/config);source=SITE_SOURCE/name
            result=subprocess.run([str(BINARY),'--topic',settings['topic'],'--validate','--jsonl',str(source)],
                                  capture_output=True,check=True,timeout=30)
            actual=[json.loads(line)['message'] for line in result.stdout.splitlines()]
            expected=[normalize(raw,settings['input']) for _,raw in jsonl_records(source)]
            self.assertEqual(actual,expected)

    def test_original_files_archive_identically_and_reimport_deduplicates(self):
        with tempfile.TemporaryDirectory(prefix='ct-real-files-') as directory:
            folder=Path(directory)
            for topic,config,source in inputs():
                selected=folder/(topic+'.jsonl');real_prefix(source,selected)
                py=folder/(topic+'-python.db');rs=folder/(topic+'-rust.db')
                settings={**load_settings(ROOT/config),'database':py}
                first=import_file(settings,selected)
                result=subprocess.run([str(BINARY),'--topic',topic,'--jsonl',str(selected),'--database',str(rs)],capture_output=True,timeout=30)
                self.assertEqual(result.returncode,1 if first['invalid'] else 0)
                before=compare_databases(py,rs)
                replay=import_file(settings,selected)
                self.assertEqual(replay['inserted'],0)
                result=subprocess.run([str(BINARY),'--topic',topic,'--jsonl',str(selected),'--database',str(rs)],capture_output=True,timeout=30)
                self.assertEqual(result.returncode,1 if first['invalid'] else 0)
                self.assertEqual(compare_databases(py,rs),before)

    def test_real_command_write_failure_rolls_back_whole_batch(self):
        settings=load_settings(ROOT/'configs/media.toml')
        raw_rows=[raw for _,raw in jsonl_records(SITE_SOURCE/'down_pic.jsonl')]
        with tempfile.TemporaryDirectory(prefix='ct-real-rollback-') as directory:
            store=Store(Path(directory)/'python.db')
            try:
                store.db.executescript("CREATE TRIGGER fail BEFORE INSERT ON records WHEN NEW.offset_id=1 BEGIN SELECT RAISE(ABORT,'write failure'); END;")
                records=[dict(topic=settings['topic'],partition=0,offset=i,raw=raw,event=normalize(raw,settings['input'])) for i,raw in enumerate(raw_rows)]
                with self.assertRaises(sqlite3.IntegrityError):
                    store.save_batch(records)
                self.assertEqual(store.db.execute('SELECT COUNT(*) FROM records').fetchone()[0],0)
                self.assertEqual(store.db.execute('SELECT COUNT(*) FROM latest_ship').fetchone()[0],0)
            finally:
                store.close()

    def test_record_identity_and_quality_contract_match_on_real_captures(self):
        with tempfile.TemporaryDirectory(prefix='ct-real-contract-') as directory:
            folder=Path(directory);py=folder/'python.db';rs=folder/'rust.db'
            for config,name in SITE_CHANNELS:
                settings={**load_settings(ROOT/config),'database':py}
                import_file(settings,SITE_SOURCE/name)
                result=subprocess.run([str(BINARY),'--topic',settings['topic'],'--jsonl',str(SITE_SOURCE/name),'--database',str(rs)],capture_output=True,timeout=30)
                self.assertEqual(result.returncode,0)
            store=Store(py)
            try:
                quality=store.quality_report();exported=store.message_records()
            finally:
                store.close()
            rust_quality=json.loads(subprocess.check_output([str(BINARY),'--quality','--database',str(rs)],text=True))
            self.assertEqual(quality,rust_quality)
            self.assertEqual((quality['records'],quality['media_commands'],quality['transport_metadata_records']),(15,10,0))
            output=folder/'events.json'
            config,name=SITE_CHANNELS[0];settings=load_settings(ROOT/config)
            result=subprocess.run([str(BINARY),'--topic',settings['topic'],'--jsonl',str(SITE_SOURCE/name),'--database',str(rs),'--export-records',str(output)],capture_output=True,timeout=30)
            self.assertEqual(result.returncode,0)
            def identity_map(rows):
                return {row['record_id']:{k:v for k,v in row.items() if k!='received_at'} for row in rows}
            self.assertEqual(identity_map(exported),identity_map(json.loads(output.read_text())))
            self.assertEqual(len(identity_map(exported)),15)
            self.assertTrue(all(row['schema_version']=='bridge-kafka.record.v1' and row['kafka_headers'] is None for row in exported))

    def test_existing_real_database_migrates_without_inventing_transport_metadata(self):
        with tempfile.TemporaryDirectory(prefix='ct-real-migrate-') as directory:
            folder=Path(directory);source=ROOT/'var/down_pic.sqlite3'
            before=None
            for language in ('python','rust'):
                path=folder/(language+'.db')
                with sqlite3.connect(f'file:{source}?mode=ro',uri=True) as original,sqlite3.connect(path) as copied:
                    original.backup(copied)
                    columns={row[1] for row in copied.execute('PRAGMA table_info(records)')}
                    for column in ('kafka_key_base64','kafka_headers_json'):
                        if column in columns:
                            copied.execute('ALTER TABLE records DROP COLUMN '+column)
                from tools.common import capture_database
                original=capture_database(path)
                if language=='python':
                    store=Store(path)
                    try:
                        quality=store.quality_report()
                    finally:
                        store.close()
                else:
                    quality=json.loads(subprocess.check_output([str(BINARY),'--quality','--database',str(path)],text=True))
                self.assertEqual(capture_database(path),original)
                self.assertEqual(quality['transport_metadata_records'],0)
                if before is not None:
                    self.assertEqual(quality,before)
                before=quality

    def test_real_ais_snapshot_failure_rolls_back_python_and_rust(self):
        topic,config=AIS_CHANNELS[0];settings=load_settings(ROOT/config)
        raw=next(raw for _,raw in jsonl_records(PREPARED/(topic+'.jsonl')) if len(str(json.loads(raw)['mmsi']))==9)
        with tempfile.TemporaryDirectory(prefix='ct-real-state-rollback-') as directory:
            folder=Path(directory);source=folder/(topic+'.jsonl');source.write_bytes(raw+b'\n')
            for language in ('python','rust'):
                path=folder/(language+'.db');store=Store(path)
                store.db.executescript("CREATE TRIGGER fail BEFORE UPDATE ON latest_ship BEGIN SELECT RAISE(ABORT,'state write failure'); END;")
                if language=='python':
                    try:
                        with self.assertRaises(sqlite3.IntegrityError):
                            store.save(topic,0,0,raw,normalize(raw,settings['input']))
                    finally:
                        store.close()
                else:
                    store.close()
                    result=subprocess.run([str(BINARY),'--topic',topic,'--jsonl',str(source),'--database',str(path)],capture_output=True,timeout=30)
                    self.assertNotEqual(result.returncode,0)
                with sqlite3.connect(path) as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM records').fetchone()[0],0)
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM latest_ship').fetchone()[0],0)

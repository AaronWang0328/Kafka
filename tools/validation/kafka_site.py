"""两个语言在独立集群、现场 Topic 原名上的端到端与恢复对照。"""
import json
from pathlib import Path
import subprocess
import sys
import tempfile

from confluent_kafka import Consumer, TopicPartition

from bridge_kafka.cli import init_topic
from bridge_kafka.store import Store
from tools.validation.compare import compare_databases
from bridge_kafka.settings import ROOT, load_settings
from tools.common import BINARY, capture_database
from tools.validation.cluster import local_cluster
from tools.common import SITE_CHANNELS as CHANNELS


def main():
    assert BINARY.is_file(), "先构建 Rust"
    with local_cluster() as bootstrap, tempfile.TemporaryDirectory(prefix="ct-site-compare-") as directory:
        folder = Path(directory)
        databases = {language: folder / (language + ".db") for language in ("python", "rust")}
        cases = []
        for config_name, original in CHANNELS:
            settings = load_settings(ROOT / config_name)
            source = ROOT / "data/source/ct_offline_batch2_20261015/pkg2/01_alarm" / original
            assert source.is_file(), f"缺少交付原件：{source}"
            topic = settings["topic"]
            client = {**settings["client"], "bootstrap.servers": bootstrap}
            init_topic({**settings, "client": client})
            config = folder / (topic + ".toml")

            def write_config(group, database, config=config, config_name=config_name, settings=settings):
                text = (ROOT / config_name).read_text().replace('bootstrap_servers = "127.0.0.1:9092"', f'bootstrap_servers = "{bootstrap}"')
                text = text.replace(f'group_id = "{settings["group"]}"', f'group_id = "{group}"')
                relative = settings["database"].relative_to(ROOT)
                text = text.replace(f'database = "{relative}"', f'database = "{database}"')
                config.write_text(text)

            write_config("seed", databases["python"])
            subprocess.run([sys.executable,str(ROOT/"manage.py"),"python","--config",str(config),"produce","--file",str(source),"--interval","0"],
                           cwd=ROOT,check=True,timeout=45)
            for language in ("python","rust"):
                group = topic + "-" + language
                write_config(group, databases[language])
                if language == "python":
                    command = [sys.executable,str(ROOT/"manage.py"),"python","--config",str(config),"consume","--idle-timeout","20"]
                    count_flag = "--max-messages"
                else:
                    command = [str(BINARY),"--bootstrap-servers",bootstrap,"--topic",topic,"--group",group,
                               "--database",str(databases[language]),"--idle-timeout","20"]
                    count_flag = "--count"
                for count, expected in ((2,2),(3,5)):
                    subprocess.run([*command,count_flag,str(count)],cwd=ROOT,check=True,timeout=45)
                    reader = Consumer({**client,"group.id":group,"enable.auto.commit":False})
                    try:
                        assert reader.committed([TopicPartition(topic,0)],timeout=10)[0].offset == expected
                    finally:
                        reader.close()
                cases.append((language,topic,config_name,command,count_flag,group,write_config))
        assert capture_database(databases["python"]) == capture_database(databases["rust"]), "两语言业务记录或船舶快照不一致"
        parity = compare_databases(databases['python'],databases['rust'])
        store=Store(databases['python'])
        try: quality=store.quality_report()
        finally: store.close()
        exported=folder/'rust-archive.json'
        rust_quality=json.loads(subprocess.check_output([str(BINARY),'--quality','--database',str(databases['rust']),'--export-records',str(exported)],text=True))
        assert quality==rust_quality and quality['transport_metadata_records']==15
        assert all(row['kafka_metadata_recorded'] for row in json.loads(exported.read_text()))
        before = {k:capture_database(p) for k,p in databases.items()}
        for language,topic,config_name,command,count_flag,group,write_config in cases:
            if language == "python":
                write_config(group+"-replay",databases[language])
            else:
                command = [group+"-replay" if item == group else item for item in command]
            subprocess.run([*command,count_flag,"5"],cwd=ROOT,check=True,timeout=45)
        for language in databases:
            assert capture_database(databases[language]) == before[language], "回放重复写入"
        captured = before["python"]
        assert len(captured["records"]) == 15 and len(captured["ships"]) == 5
        assert all(row[5] == "valid" for row in captured["records"])
        assert sum(row[6]["message_type"] == "media_command" for row in captured["records"]) == 10
        report = {"result":"PASS","languages":["python","rust"],"topics":["ABNORMAL_TOPIC","down_pic","down_pic_plus"],
                  "messages_per_language":15,"invalid":0,"transport_metadata_parity":"PASS","quality_parity":"PASS","quality":quality,"raw_and_normalized_parity":"PASS","snapshot_parity":"PASS",
                  "offset_resume":"PASS","replay_dedup":"PASS","command_ais_isolation":"PASS",
                  "test_cluster":"isolated loopback, destroyed after test"}
        (ROOT/'reports/validation').mkdir(parents=True,exist_ok=True)
        (ROOT / "reports/validation/site-protocols-integration.json").write_text(json.dumps(report,ensure_ascii=False,indent=2))
        print(json.dumps(report,ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
    from tools.validation.kafka_ais import main as continuous_main
    continuous_main()

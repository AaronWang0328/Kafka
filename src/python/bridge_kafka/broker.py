"""项目内单节点 Kafka 管理；仅绑定本机，不修改系统 Java 或系统服务。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tarfile
import time

from .settings import ROOT

VERSION = "4.2.2"
RUNTIME = ROOT / ".runtime"
DIST = RUNTIME / f"kafka_2.13-{VERSION}"
CONFIG = RUNTIME / "server.properties"
DATA = ROOT / "var" / "kraft-data"
PIDFILE = RUNTIME / "broker.pid"
LOGFILE = ROOT / "var" / "broker.log"


def java_env():
    env = os.environ.copy()
    java_home = Path(sys.prefix)
    if not (java_home / "bin" / "java").exists():
        raise RuntimeError("请先 conda activate kafka（环境内需要 Java 17）")
    # Kafka 启动脚本默认启用 JMX agent；本地框架不需要额外管理监听端口。
    env.pop("JMX_PORT", None)
    env.update(JAVA_HOME=str(java_home), KAFKA_HEAP_OPTS="-Xms256m -Xmx512m",
               KAFKA_JMX_OPTS="-Djava.awt.headless=true", LOG_DIR=str(ROOT / "var" / "broker-logs"))
    return env


def install():
    if (DIST / "bin" / "kafka-server-start.sh").exists():
        print(f"Kafka 已安装：{DIST}")
        return
    RUNTIME.mkdir(exist_ok=True)
    name = f"kafka_2.13-{VERSION}.tgz"
    archive = RUNTIME / name
    checksum = RUNTIME / f"{name}.sha512"
    bases = (f"https://dlcdn.apache.org/kafka/{VERSION}", f"https://archive.apache.org/dist/kafka/{VERSION}")
    for path in (archive, checksum):
        if not path.exists():
            # 临时文件下载成功后才重命名，避免把中断下载当成完整包。
            temporary = Path(str(path) + ".part")
            for base in bases:
                result = subprocess.run(["curl", "--fail", "--location", "--retry", "2", "--connect-timeout", "15",
                    "--max-time", "600", "--silent", "--show-error", f"{base}/{path.name}", "-o", str(temporary)])
                if result.returncode == 0:
                    break
            else:
                raise RuntimeError(f"下载失败：{path.name}")
            temporary.replace(path)
    checksum_text = checksum.read_text().split(":", 1)[-1]
    expected = "".join(re.findall(r"\b[0-9a-fA-F]{8}\b", checksum_text)).lower()
    with archive.open("rb") as handle:
        actual = hashlib.file_digest(handle, "sha512").hexdigest()
    if len(expected) != 128 or actual != expected:
        raise RuntimeError("Kafka 包 SHA-512 校验失败；请检查 .runtime 下载文件")
    with tarfile.open(archive) as handle:
        handle.extractall(RUNTIME, filter="data")
    print(f"SHA-512 校验通过，Kafka 已安装：{DIST}")


def prepare():
    if not DIST.exists():
        raise RuntimeError("先运行 python manage.py broker install")
    DATA.mkdir(parents=True, exist_ok=True)
    CONFIG.write_text(f"""# Generated: local single-node KRaft, loopback only.
process.roles=broker,controller
node.id=1
controller.listener.names=CONTROLLER
controller.quorum.bootstrap.servers=127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={DATA}
num.partitions=1
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=104857600
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
""", encoding="utf-8")
    if not (DATA / "meta.properties").exists():
        cluster_id = subprocess.check_output([str(DIST / "bin/kafka-storage.sh"), "random-uuid"],
                                            env=java_env(), text=True).strip()
        subprocess.run([str(DIST / "bin/kafka-storage.sh"), "format", "--standalone", "-t", cluster_id,
                        "-c", str(CONFIG)], env=java_env(), check=True)


def owned_pid():
    if not PIDFILE.exists():
        return None
    try:
        pid = int(PIDFILE.read_text().strip())
        command = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        if str(CONFIG).encode() in command:
            return pid
    except (OSError, ValueError):
        pass
    return None


def ready(timeout=2):
    from confluent_kafka.admin import AdminClient
    try:
        client = AdminClient({"bootstrap.servers": "127.0.0.1:9092", "log_level": 0})
        return bool(client.list_topics(timeout=timeout).brokers)
    except Exception:
        return False


def start():
    if owned_pid():
        if ready():
            print(f"本项目 Kafka 已运行，PID={owned_pid()}")
            return
        raise RuntimeError(f"本项目 Kafka 进程存在但未就绪，请查看 {LOGFILE}")
    for port in (9092, 9093):
        with socket.socket() as probe:
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                raise RuntimeError(f"本机端口 {port} 已被占用，未启动新进程")
    prepare()
    with LOGFILE.open("ab") as handle:
        process = subprocess.Popen([str(DIST / "bin/kafka-server-start.sh"), str(CONFIG)], env=java_env(),
            cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
    PIDFILE.write_text(str(process.pid))
    for _ in range(45):
        if process.poll() is not None:
            raise RuntimeError(f"Kafka 启动失败，请查看 {LOGFILE}")
        if ready(1):
            print(f"Kafka 已就绪：127.0.0.1:9092，PID={process.pid}，日志={LOGFILE}")
            return
        time.sleep(0.5)
    # 未能就绪时回收本次启动的进程，便于下次重试。
    process.terminate()
    raise RuntimeError(f"Kafka 启动超时，请查看 {LOGFILE}")


def stop():
    pid = owned_pid()
    if pid is None:
        print("本项目 Kafka 未运行")
        return
    os.kill(pid, signal.SIGTERM)
    for _ in range(60):
        if owned_pid() is None:
            PIDFILE.unlink(missing_ok=True)
            print("Kafka 已停止；消息数据和本地数据库均保留")
            return
        time.sleep(0.5)
    raise RuntimeError(f"Kafka 尚未退出，PID={pid}，请查看日志")


def status():
    result = {"pid": owned_pid(), "ready": ready(), "bootstrap_servers": "127.0.0.1:9092",
              "log": str(LOGFILE), "data": str(DATA)}
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["pid"] and result["ready"] else 1


def main():
    parser = argparse.ArgumentParser(description="本项目单节点 Kafka 管理")
    parser.add_argument("command", choices=["install", "start", "stop", "status"])
    args = parser.parse_args()
    try:
        return {"install": install, "start": start, "stop": stop, "status": status}[args.command]() or 0
    except Exception as exc:
        print(f"失败：{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

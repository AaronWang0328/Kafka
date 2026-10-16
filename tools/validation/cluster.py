"""独立本机 Kafka 测试集群，可使用现场 Topic 原名而不污染业务集群。"""
from contextlib import contextmanager
import fcntl
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

from confluent_kafka.admin import AdminClient
from bridge_kafka.broker import DIST, java_env


@contextmanager
def local_cluster():
    # 验证脚本与性能脚本串行使用集群，避免并行测试占用资源污染跑分。
    with open('/tmp/changtai-kafka-test.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with _local_cluster() as bootstrap:
            yield bootstrap


@contextmanager
def _local_cluster():
    with tempfile.TemporaryDirectory(prefix="ct-kafka-isolated-") as directory:
        root = Path(directory)
        probes = [socket.socket(), socket.socket()]
        for probe in probes:
            probe.bind(("127.0.0.1", 0))
        broker_port, controller_port = [probe.getsockname()[1] for probe in probes]
        bootstrap = f"127.0.0.1:{broker_port}"
        config = root / "server.properties"
        config.write_text(f'''process.roles=broker,controller
node.id=1
controller.listener.names=CONTROLLER
controller.quorum.bootstrap.servers=127.0.0.1:{controller_port}
listeners=PLAINTEXT://127.0.0.1:{broker_port},CONTROLLER://127.0.0.1:{controller_port}
advertised.listeners=PLAINTEXT://{bootstrap}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs={root / 'data'}
num.partitions=1
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
log.segment.bytes=104857600
''')
        env = java_env()
        env["LOG_DIR"] = str(root / "logs")
        cluster_id = subprocess.check_output([str(DIST / "bin/kafka-storage.sh"), "random-uuid"], env=env, text=True).strip()
        subprocess.run([str(DIST / "bin/kafka-storage.sh"), "format", "--standalone", "-t", cluster_id,
                        "-c", str(config)], env=env, check=True, stdout=subprocess.DEVNULL, timeout=30)
        for probe in probes:
            probe.close()
        with (root / "broker.log").open("wb") as log:
            process = subprocess.Popen([str(DIST / "bin/kafka-server-start.sh"), str(config)],
                                       env=env, stdout=log, stderr=log, start_new_session=True)
            try:
                admin = AdminClient({"bootstrap.servers": bootstrap, "log_level": 0})
                deadline = time.monotonic() + 45
                while True:
                    if process.poll() is not None:
                        raise RuntimeError("隔离 Kafka 启动失败：" + (root / "broker.log").read_text(errors="replace")[-1800:])
                    try:
                        if admin.list_topics(timeout=1).brokers:
                            break
                    except Exception:
                        pass
                    if time.monotonic() >= deadline:
                        raise RuntimeError("隔离 Kafka 启动超时")
                    time.sleep(0.1)
                yield bootstrap
            finally:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)

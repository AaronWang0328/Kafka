#!/usr/bin/env python3
"""常泰桥梁 Kafka 项目的统一运行入口。"""
from pathlib import Path
import importlib
import os
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT/'src/python'))
sys.path.insert(0, str(ROOT))

HELP = '''用法：python manage.py <命令> [参数]

  python          运行 Python 客户端（inspect / consume / produce / import-file 等）
  rust            运行 Rust 客户端，后接 Rust 参数
  broker          本机 Kafka 安装、启动、停止、状态
  prepare         从真实 AIS 补充包生成按 Topic 划分的输入
  dashboard       从现有库生成可视化验收页（只读核对，可加 --serve）
  summary         根据已有证据生成工作成果与性能质量总结 HTML
  build-rust      编译 Rust 客户端
  verify          用交付原件执行回归验证
  verify --full   全量 AIS 导入、逐字段比较及去重验证；临时库自动清理
  integration     独立本机 Kafka，验证五个实际 Topic 的恢复与去重
  throughput      全量真实 AIS 的 Kafka 吞吐比较
  throughput --file  全量真实 AIS 的文件到 SQLite 吞吐比较

子命令选项：python manage.py <命令> --help
'''


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ('-h','--help'):
        print(HELP)
        return 0
    command, args = sys.argv[1], sys.argv[2:]
    os.chdir(ROOT)
    if command == 'build-rust':
        return subprocess.call(['bash',str(ROOT/'tools/build_rust.sh'),*args])
    if command == 'rust':
        from tools.common import BINARY
        if not BINARY.is_file():
            raise RuntimeError('请先执行 python manage.py build-rust --locked --offline')
        return subprocess.call([str(BINARY),*args])
    if command in ('verify','integration') and args == ['--help']:
        print(HELP)
        return 0
    if command == 'verify' and '--full' not in args:
        import unittest
        if args:
            raise RuntimeError('verify 使用真实交付文件；全量核对请加 --full')
        suite = unittest.defaultTestLoader.discover(str(ROOT/'tools/validation'),pattern='test_real.py')
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if command == 'integration':
        if args:
            raise RuntimeError('integration 不接受额外参数')
        from tools.validation.kafka_site import main as site
        from tools.validation.kafka_ais import main as ais
        sys.argv = [sys.argv[0],*args]
        site()
        ais()
        return 0
    if command == 'throughput':
        mode = 'file' if '--file' in args else 'kafka'
        args = [arg for arg in args if arg != '--file']
        module = 'tools.performance.'+mode
    elif command == 'verify':
        args = [arg for arg in args if arg != '--full']
        module = 'tools.validation.full'
    else:
        module = {'python':'bridge_kafka.cli','broker':'bridge_kafka.broker','prepare':'bridge_kafka.supplement',
                  'dashboard':'tools.inspection.dashboard','summary':'tools.inspection.summary'}.get(command)
        if module is None:
            raise RuntimeError('未知命令；使用 python manage.py --help 查看入口')
    sys.argv = [sys.argv[0],*args]
    return importlib.import_module(module).main() or 0


if __name__ == '__main__':
    raise SystemExit(main())

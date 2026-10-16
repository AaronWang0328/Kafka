# 常泰桥梁 Kafka

基于真实交付数据的 Python / Rust 接入项目。先看 [工作成果、性能与数据质量总结](docs/Kafka_工作成果与性能质量总结.html)；业务依据是 [原始功能需求](docs/requirements/常泰桥梁_功能需求说明.html)。本文件集中说明目录、运行和复核方法，其余重复说明已归并。

## Git 提交范围

仓库提交当前源码、配置、依赖声明、总结与原始需求、精简验证和性能 JSON。原始压缩包、视频/点云/AIS 数据、数据库、运行环境、日志、会话与本机凭证目录均由 `.gitignore` 排除；这些文件继续保存在本机。克隆后若需运行真实数据验证，应另行恢复 `data/` 并准备本机环境。

## 目录与保留原则

| 目录 | 内容 |
|---|---|
| `src/python/bridge_kafka/` | 当前 Python：解析、消费、留档、状态与诊断 |
| `src/rust/` | 当前 Rust，支持相同的已确认现场协议 |
| `tools/` | 构建、真实数据验证、吞吐与总结/验收页生成；HTML 模板放在 `inspection/templates/` |
| `configs/` | 五个实际 Topic 的配置 |
| `environment/` | Python 3.11 / Java 17 环境与依赖锁定 |
| `data/archives/` | 原始压缩分卷和 AIS 补充包 |
| `data/source/` | 交付原件：告警、连续 AIS、视频、点云、CSV 和供应商说明 |
| `data/prepared/ais-supplement/` | 按 Topic 拆分的真实原行、CSV 独有记录、来源行号与 SHA 清单 |
| `docs/` | 一份总结 HTML、原始需求 HTML、留档导出契约 |
| `reports/validation/` | 全量一致性、来源核查、Kafka 恢复等必要证据 |
| `reports/performance/` | 当前及有对照意义的历史性能 JSON；latest 是指向原报告的链接 |
| `reports/fusion/` | 师弟实现适用项的融合与质量证据 |
| `reports/dashboard/receipt.json` | 最近一次当前库只读核对凭据；验收页面按需生成 |
| `references/` | 师弟原始 ZIP；已核实与原解压副本相同，只保留原包 |
| `var/`、`var/rust/` | Python / Rust 当前数据库与本机 Kafka 数据 |
| `.runtime/` | Kafka、Rust 工具链、离线依赖包及当前可执行文件 |

原始数据、原始需求、当前数据库和必要运行依赖保留。自有说明合并到本文件和总结页；交付原件中的说明、依赖许可证属于来源或运行材料。清理记录见 [reports/cleanup.json](reports/cleanup.json)。

## 运行入口

所有命令在项目根目录运行：

```bash
conda activate kafka
python manage.py --help
python manage.py python inspect
python manage.py python quality
python manage.py python --config configs/ais-platform.toml quality
python manage.py rust --quality --database var/rust/ais_parsing_ori.sqlite3
```

Python 使用现有 kafka 环境（Python 3.11、Java 17、confluent-kafka）；实测依赖版本在性能报告和环境锁定文件中。Rust 构建和运行：

```bash
python manage.py build-rust --locked --offline
python manage.py rust --help
```

可执行文件为 `.runtime/rust-target/release/bridge-kafka`。编译中间文件已清理，后续构建会从保留的离线依赖包重新展开和编译。

## 五个实际 Topic

| Topic | 配置 | 真实来源依据 |
|---|---|---|
| `ais_parsing_ori` | `configs/ais-origin.toml`，默认 | 补充包桥区 JSONL，232698 行 |
| `REALTIME_TOPIC` | `configs/ais-platform.toml` | 同一 JSONL，35917 行 |
| `ABNORMAL_TOPIC` | `configs/alarm.toml` | real_alarm_raw.jsonl，5 条原始 Kafka 留档 |
| `down_pic` | `configs/media.toml` | down_pic.jsonl，5 条原始 Kafka 留档 |
| `down_pic_plus` | `configs/media-plus.toml` | down_pic_plus.jsonl，5 条原始 Kafka 留档 |

[原件与配置核对](reports/validation/source-evidence.json)记录实际行数、文件哈希和字节一致性。连续 AIS 是供应商逐船投影导出，尚无完整现网 Kafka 原始 value/key/headers；三个告警/媒体留档带分区、offset、时间和 value，但未提供原始 key/headers。本地同名 Topic 不表示已接入现网。

## 数据导入与 Kafka 回放

```bash
python manage.py prepare
python manage.py python import-file --file data/prepared/ais-supplement/ais_parsing_ori.jsonl
python manage.py rust --topic ais_parsing_ori --jsonl data/prepared/ais-supplement/ais_parsing_ori.jsonl --database var/rust/ais_parsing_ori.sqlite3
```

prepare 保持 JSONL 原行字节与顺序，CSV 仅另存不重合的 146 条，保留原列和来源行号。离线导入返回 1 时先看 invalid 数：真实异常已留档，不表示整份导入失败。两个 AIS 来源默认独立数据库。

```bash
python manage.py broker start
python manage.py python init
python manage.py python produce --file data/prepared/ais-supplement/ais_parsing_ori.jsonl --interval 0 --allow-invalid
python manage.py python consume
python manage.py broker status
python manage.py broker stop
```

其余 Topic 在 python 后加对应的 `--config configs/<配置>.toml`。produce 必须指定真实文件，不生成业务数据；allow-invalid 显式回放包内已有异常。Kafka 为单节点、单分区、单副本，仅监听 127.0.0.1:9092。

## 可视化验收与总结

```bash
python manage.py summary
python manage.py dashboard --serve
```

总结写入 [docs/Kafka_工作成果与性能质量总结.html](docs/Kafka_工作成果与性能质量总结.html)，可离线阅读与打印。生成只使用已有报告，不重新压测。

验收服务地址是 http://127.0.0.1:8765/，按 Ctrl+C 结束。生成时只读逐条核对五个 Topic 的当前两版库，展示实际 MMSI、真实首尾记录、异常、最终字段、需求对照与性能证据；不插值、不模拟运动、不发布消息。单独执行 dashboard 可生成 `reports/dashboard/index.html` 供双击查看。页面和截图属于派生产物，平时无需保留。

验收页是生成时快照，浏览器刷新不会重新核对数据库。当前库比较原文、来源位置、错误、规范化字段、传输元数据与最终状态，只排除执行接收时钟和 JSON 键顺序，无数值容差；源码指纹不符的旧报告不能作为当前版本结论。

## 验证与性能

```bash
python manage.py verify
python manage.py verify --full
python manage.py integration
python manage.py throughput --rounds 4 --warmup 5000
python manage.py throughput --file --rounds 4 --warmup 5000
```

全量验证是 268615 条 JSONL 加 146 条 CSV 独有观测，共 268761 条。集成验证覆盖五个 Topic 的独立 Kafka 回放、提交恢复与去重。压测每 Topic 完整 JSONL 每行每轮一次，两版均一个处理线程、2048 条/50ms、SQLite WAL+FULL、落盘后同步提交；临时库和测试集群结束后清理。历史四方对照保留在报告中，旧基线代码已删除，不重复保留压测日志或 latest 的 JSON 副本。

成功压测只保留含逐轮数值的正式 JSON，latest 使用符号链接；失败运行保留日志便于排查。本地吞吐不代表现网延迟或 Broker 极限。两版一致性范围为已确认现场协议和相同默认条件；Python 的通用自定义字段映射与自定义质量配置不属于已验证的跨语言对比范围。

## 数据语义与验收边界

- AIS 持续接收，不等待告警；告警每条单次触发约 20 秒录像，前端决定持续显示多久。E02/E05 的交付格式与原始报文不作为两个独立事件，告警不同 MMSI 共 8 个；连续 AIS 不按这 8 个名单过滤。
- 原解析侧 last_updated 为秒，向下取整至毫秒作为 AIS 来源时刻；平台 last_updated 来自毫秒 createDate，保留为观测时间，不能冒充 AIS 采集时刻。recv 显式按北京时间 UTC+8 解释。重复快照不刷新未更新的位置时刻。
- 速度为节，长宽为米；艏向和航迹向分开。零尺寸、空船名及不可用角度记为未知，原值留档。八位 MMSI 34682506 的 1236 条异常不补零、不删除。
- 型深、坐标基准、平台 AIS 采集时间、媒体布尔选项含义和现场匹配门限未确认时保留未知。不用交付包裁剪范围冒充业务门限，不按船宽比例估高。
- 原始 value 完整留档；新消费记录实际 key 和有序 headers，旧库/离线未记录的元数据保持 null，不伪造。实际回放 MMSI key 不冒充现网原始 key；无非空现网 headers 样本。
- [留档导出契约](docs/contracts/archive-record-v1.schema.json)使用版本和来源稳定标识，不更改上游 value 或业务事件 ID。媒体展示和导出去除凭证。
- 当前接入实现不包含目标检测、相机动作、录像、点云测高或最终融合；包内已有视频不是本程序产物。视频文件名为秒级系统段起始时间，OSD 为相机时钟，需双重校时；告警保留毫秒，点云按 recv_ms 对齐。现网连接、VTS 接入方式及端到端效果仍待联调验收。

如需重建批次二原件，合并 `data/archives/` 下 .00/.01 后解压至 `data/source/ct_offline_batch2_20261015/`；合并 SHA256 应为 f9750af21041fb080d959bdf83e7fde1a19d192a0f2dd24e675ddbc8a7a6d647。原始分卷和补充包仍保留，重复的合并大包已清除。

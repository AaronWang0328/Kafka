"""用已有真实数据报告生成可离线阅读的总结；不重新压测、不写业务库。"""
from collections import Counter
from datetime import datetime
from html import escape
import hashlib
import json
import os
from pathlib import Path

from bridge_kafka.settings import ROOT
from tools.inspection.dashboard import digest_source, requirements

OUTPUT = ROOT/'docs/Kafka_工作成果与性能质量总结.html'


def num(value):
    return f'{value:,.0f}'


def link(path, text):
    return f'<a href="{escape(os.path.relpath(ROOT/path, OUTPUT.parent), quote=True)}">{escape(text)}</a>'


def table(headings, rows):
    return '<div class="table-wrap"><table><thead><tr>'+''.join(f'<th>{h}</th>' for h in headings)+'</tr></thead><tbody>'+''.join('<tr>'+''.join(f'<td>{v}</td>' for v in row)+'</tr>' for row in rows)+'</tbody></table></div>'


def section(id_, title, body):
    return f'<section id="{id_}"><h2>{title}</h2>{body}</section>'


def generate():
    evidence = []

    def read(path, label):
        raw = (ROOT/path).read_bytes()
        data = json.loads(raw)
        evidence.append((path, label, data, hashlib.sha256(raw).hexdigest()))
        return data

    fusion = read('reports/fusion/peer-integration-20261016.json', '适用项融合与质量汇总')
    perf = read(fusion['performance_report'], '最新四轮四方性能原始报告')
    full = read('reports/validation/ais-offline-parity.json', '全量新库核对、重导入与质量')
    audit = read('reports/validation/ais-supplement-audit.json', 'AIS 补充包原始核查')
    ais = read('reports/validation/ais-kafka-parity.json', '两个 AIS Topic 的 Kafka 集成')
    site = read('reports/validation/site-protocols-integration.json', '告警及媒体三个 Topic 的集成')
    receipt = read('reports/dashboard/receipt.json', '最近一次当前库只读核对')
    read('reports/validation/source-evidence.json', '五 Topic 原件、配置与输入字节核对')
    rh = read('reports/performance/rust-optimization/results.json', 'Rust 早期优化记录')
    ph = read('reports/performance/python-optimization/results.json', 'Python 早期优化记录')
    code = digest_source()
    matched = all(d['source_code_sha256'] == code for d in (fusion, perf, full, receipt))
    qc = [c['quality'] for c in full['channels']]
    total, valid, invalid = (sum(q['records'] for q in qc), sum(q['statuses']['valid'] for q in qc), sum(q['statuses']['invalid'] for q in qc))
    observed = sum(q['ais_events'] for q in qc)
    known, unknown = sum(q['known_ais_times'] for q in qc), sum(q['unknown_ais_times'] for q in qc)
    assert total == valid+invalid == fusion['validation']['full_records']
    assert known+unknown == observed == valid
    assert sum(c['messages_per_run'] for c in perf['channels']) == full['bridge_rows']
    blocks = []
    blocks.append(section('overview', '01 / 当前成果与边界', '''
<p class="intro">已完成基于真实交付数据的 Kafka 接入实现、Python / Rust 双版本统一、恢复与一致性验证、吞吐优化，以及目录和模拟数据清理。当前输出包括可追溯消息留档、规范化事件、船舶状态和质量诊断，供媒体留存及雷达融合继续使用。</p>
<div class="split"><div class="callout"><b>本地已验证</b><p>五个真实 Topic 的协议解析、原文保留、事务落盘、进度恢复与重放去重，以及两种语言结果一致。</p></div><div class="callout pending"><b>仍需现场和下游验收</b><p>现网连续 AIS 原始报文与连接联调、时间和坐标等资料确认，以及真实录像、点云测高与最终融合效果。</p></div></div>'''))
    flow = '<div class="flow">'+''.join(f'<div><small>0{i+1}</small><b>{title}</b><span>{detail}</span></div>' for i,(title,detail) in enumerate([
        ('真实输入 / Kafka','交付文件回放、已确认协议'),('协议与质量校验','类型、单位、时间和异常'),
        ('原文与船舶状态','按字段时间合并，保留来源'),('整批事务落盘','SQLite WAL + FULL'),
        ('同步提交进度','先落盘、后提交、重放去重'),('查询与导出','状态、事件、错误与追溯')]))+'</div>'
    topic_rows = [
        ('ais_parsing_ori','持续 AIS：原始解析侧','身份、登记尺度、动态位置；last_updated 是确认的 AIS 来源时刻。'),
        ('REALTIME_TOPIC','持续 AIS：平台侧','身份、尺度、动态位置；createDate 仅保留为平台观测时间。'),
        ('ABNORMAL_TOPIC','业务告警和船舶快照','保留事件标识、类型、毫秒告警时刻和位置。'),
        ('down_pic','位置与媒体路径指令','解析归档；不当成 AIS 更新船舶状态。'),
        ('down_pic_plus','相机与媒体路径指令','解析归档；展示和导出移除相机凭证，原文保留。')]
    blocks.append(section('workflow', '02 / 代码流程与 Kafka 的位置',
        '<p>连续 AIS 持续提供船舶输入，不等待告警；告警独立触发下游媒体留存。Kafka 承担消息传输、缓冲和消费进度管理；Python / Rust 客户端完成解析、留档与状态维护。</p>'+flow+
        table(['实际 Topic','角色','当前处理'],[(f'<code>{t}</code>',r,b) for t,r,b in topic_rows])+'<p>'+link('reports/validation/source-evidence.json','查看五个 Topic 的原件、实际行数和配置核对证据')+'</p>'+'''
<p class="caption">名称来自交付证据。连续 AIS 当前是供应商逐船投影导出，已做本地 Kafka 回放；不能据此声称已取得现网连续 AIS 原始 value、key、headers 和实时连接。</p>
<div class="split"><div><h3>功能一的后续链路</h3><p>告警 → 相机 / 目标匹配 → 单次约 20 秒标注录像 → 文件检查及访问。前端决定告警持续显示多久。本模块当前解析和留档消息，尚无自己生成的 JPG / MP4 产物。</p></div><div><h3>功能二的后续链路</h3><p>持续 AIS 状态 → 按位置和时间关联雷达目标 → 登记尺度交叉校验 → 可信高度。本模块提供 AIS 输入，未实现点云测高和最终融合算法。</p></div></div>'''))
    work = [
        ('核实需求与真实数据','按 HTML 1.2、3.2–3.5、4.1–4.6 梳理告警留存与持续 AIS 融合两条链路，核查原批次二交付包和 AIS 补充包。'),
        ('统一两版业务结果','Python / Rust 使用相同字段、单位、时间含义、错误码、合并规则、去重与导出契约。两个 AIS 来源独立存储，避免平台时间覆盖确认的 AIS 来源时间。'),
        ('可靠持久化与恢复','有效和异常消息均保留原文；原文、规范化结果和状态整批事务写入，成功后同步提交 Kafka 进度；失败整批回滚，重放按来源位置去重。'),
        ('Rust 性能优化','减少每条消息整份状态拷贝；按字段选择更新，批末形成最终状态；复用 SQL、JSON 和 Base64 缓冲。业务语义和持久化条件保持。'),
        ('Python 性能优化','减少重复日期解析与格式化，复用编码器和游标；流配置提前校验，批内原地合并，批末汇总状态，减少重复计算。'),
        ('融合师弟适用项','两版采用紧凑 JSON、带版本导出包装、来源稳定记录标识、实际 key / 有序 headers 留档、确定性质量诊断；保持先持久化后提交。'),
        ('整理代码与删除模拟内容','源码归入 src/python、src/rust，工具归入 tools，数据与报告分开管理；统一 manage.py；移除模拟器、虚构样例、旧实现快照及无用编译产物，保留原始交付包和师弟原件。'),
        ('可视化验收','只读展示真实 Topic / MMSI / 首尾记录、异常来源、两版最终字段对比、HTML 要求矩阵和证据指纹。页面为生成时快照。')]
    blocks.append(section('work','03 / 已完成工作',table(['工作项','具体成果'],work)+
        '<div class="callout pending"><b>参考方案的取舍</b><p>未采用模拟 AIS、自定 changtai.* / DLQ Topic、未经标定的固定匹配门限或“船宽 × 0.725”估高。缺失时间、坐标基准、型深继续保持未知。</p></div>'+link('reports/fusion/peer-integration-20261016.json','融合取舍与验证证据')))
    perf_rows, storage_rows, charts, rounds = [], [], [], []
    gains = {c['topic']:c for c in fusion['channels']}
    for channel in perf['channels']:
        t,s = channel['topic'],channel['summary']
        maximum = max(v['messages_per_second'] for v in s.values())
        bars = []
        for key,label in [('python-before','Python 融合前'),('python','Python 当前'),('rust-before','Rust 融合前'),('rust','Rust 当前')]:
            rate = s[key]['messages_per_second']
            cls = ('rust ' if key.startswith('rust') else '')+('before' if key.endswith('before') else '')
            bars.append(f'<div class="bar-row"><div><span>{label}</span><b>{num(rate)} <small>条/秒</small></b></div><div class="track"><i class="bar {cls}" style="width:{rate/maximum*100:.6f}%"></i></div></div>')
        charts.append(f'<div class="panel"><h3><code>{t}</code></h3><p class="caption">每轮 {num(channel["messages_per_run"])} 条 · 各四轮中位数</p>{"".join(bars)}</div>')
        for language,label in [('python','Python'),('rust','Rust')]:
            before,after,gain = s[language+'-before'],s[language],gains[t][language]
            ratio = f'{s["rust"]["messages_per_second"]/s["python"]["messages_per_second"]:.2f} 倍' if language=='rust' else '—'
            perf_rows.append([f'<code>{t}</code>',label,num(before['messages_per_second']),num(after['messages_per_second']),f'+{gain["throughput_gain_percent"]:.1f}%',ratio])
            storage_rows.append([f'<code>{t}</code>',label,f'{before["client_cpu_seconds"]:.3f} → {after["client_cpu_seconds"]:.3f}',f'−{gain["cpu_reduction_percent"]:.1f}%',f'{before["database_bytes"]/1048576:.2f} → {after["database_bytes"]/1048576:.2f}',f'−{gain["new_database_reduction_percent"]:.1f}%'])
        rows = [[escape(r['label']),escape(r['language']),f'{r["seconds"]:.3f}',num(r['messages_per_second']),f'{r["client_cpu_seconds"]:.3f}',f'{r["database_bytes"]/1048576:.2f}'] for r in channel['runs']]
        rounds.append(f'<details><summary>{t} · 展开四种实现的逐轮数据</summary>'+table(['轮次','实现','总耗时 (s)','吞吐 (条/秒)','客户端 CPU (s)','库文件 (MiB)'],rows)+'</details>')
    native = perf['native_versions']
    blocks.append(section('performance','04 / 最新性能：同次四方对照',
        '<p>“融合前 / 当前”指本轮师弟适用项融合前后，各实现交替运行四轮。每个 Topic 单独回放完整桥区 JSONL，展示进程启动至退出的吞吐中位数。</p><div class="split">'+''.join(charts)+'</div>'+
        table(['Topic','语言','融合前 (条/秒)','当前 (条/秒)','本轮提升','当前 Rust / Python'],perf_rows)+
        f'<div class="callout"><b>公平比较条件</b><p>独立本机 Kafka 4.2.2；单 Broker、单分区、单副本，一个处理线程；2048 条 / 50ms；SQLite WAL+FULL，落盘后同步提交。各实现预热 5000 条。接收、解析、原文与状态落盘、提交和退出均计时，生产者准备积压及父进程核验不计时。相同 librdkafka {native["librdkafka"]}、SQLite {native["sqlite"]}。</p></div>'+
        table(['Topic','语言','客户端 CPU 秒：前 → 后','CPU 降幅','新建库 MiB：前 → 后','库文件降幅'],storage_rows)+
        '<p class="caption">规范化 JSON UTF-8 字节数减少约 6.3%–6.4%，原始 value 未删减。数据库比例适用于本批新建库；旧库没有重写历史 JSON，不能声称已回收相同比例空间。1 MiB = 1,048,576 字节。本地客户端积压回放吞吐不代表现网延迟、Broker 极限或多节点容量。</p>'+''.join(rounds)+'<p>'+link(fusion['performance_report'],'最新四轮性能原始报告')+'</p>'))
    history = [[ 'Rust 状态与缓冲优化',c['topic'],f'{num(c["rust_before"])} → {num(c["rust_after"])}',f'+{(c["rust_speedup"]-1)*100:.1f}%'] for c in rh['gains']]
    history += [['Python 日期与合并优化',c['topic'],f'{num(c["python_before"])} → {num(c["python_after"])}',f'+{(c["python_throughput_gain"]-1)*100:.1f}%'] for c in ph['channels']]
    blocks.append('<details><summary>展开更早阶段的优化结果</summary><p>以下为更早的独立同次对照，源码和测试批次不同。用于说明优化过程，不跨阶段连乘，不当成当前版本分数。旧基线源码已删除，历史报告保留。历史 JSON 内的旧目录名是当时位置，当前原始报告入口如下。</p>'+table(['历史阶段','Topic','吞吐 (条/秒)：前 → 后','同次提升'],history)+'<p>'+link('reports/performance/rust-optimization/results.json','Rust 历史汇总')+' · '+link('reports/performance/continuous-ais/20261016-184921/results.json','Rust 历史四轮原始报告')+' · '+link('reports/performance/python-optimization/results.json','Python 历史汇总')+' · '+link('reports/performance/continuous-ais/20261016-190925/results.json','Python 历史四轮原始报告')+'</p></details>')
    count = Counter(p.suffix.lower() for p in (ROOT/'data/source/ct_offline_batch2_20261015/pkg2').rglob('*') if p.is_file())
    source_rows = [
        ('原批次二交付包',f'{count[".mp4"]} MP4、{count[".pcd"]} PCD、{count[".jpg"]} JPG、{count[".csv"]} CSV、{count[".jsonl"]} JSONL 及说明','包含视频、点云、告警与媒体原始留档。包内视频是供应商已有产物。'),
        ('桥区连续 AIS JSONL',num(full['bridge_rows'])+' 行','原解析侧 232,698 行，平台侧 35,917 行；最新吞吐的输入口径。'),
        ('告警 MMSI 轨迹 CSV',num(audit['csv']['rows'])+' 行',f'{num(full["csv_overlap_excluded"])} 行与 JSONL 重合，只补 {num(full["csv_extra_rows"])} 条独有观测。'),
        ('全量质量与一致性',num(total)+' 条','JSONL + CSV 独有记录；含真实异常。此口径比吞吐输入多 146 条。'),
        ('告警 / 媒体 Kafka 验证',f'{site["messages_per_language"]} 条 / 语言','三个实际 Topic 各 5 条原始留档；另有两个 AIS Topic 验证。')]
    quality_body = table(['来源 / 口径','规模','说明'],source_rows)+'''
<p>来源接收窗口为北京时间 <b>2026-10-15 16:25:20.873–16:49:59.746</b>。桥区 JSONL 共 249 个不同标识，其中 248 个符合九位 MMSI 格式；不局限于告警涉及的 8 个 MMSI。E02 / E05 的交付格式与原始报文是同一事件的两种表示。</p>'''+table(['Topic','全部行','协议有效','真实异常','当前状态数'],[[c['topic'],num(c['quality']['records']),num(c['quality']['statuses']['valid']),num(c['quality']['statuses']['invalid']),num(c['quality']['ships'])] for c in full['channels']])+f'''
<div class="callout"><b>协议有效 {num(valid)} / {num(total)}，占 {valid/total*100:.2f}%</b><p>{num(invalid)} 条异常来自真实八位标识 34682506；不补零、不静默删除，完整留档并记录错误。协议有效率不是融合成功率或系统准确率。不同 Topic 的船舶状态数有重叠，不能相加当成独立船舶数。</p></div>'''
    gaps = [('确认的 AIS 采集时间',known,'仅统计已确认为 AIS 来源时刻的有效观测。'),
            ('未知 AIS 采集时间',unknown,'原解析侧 CSV 独有 119 条 + 平台侧 35,892 条；createDate 不冒充采集时间。'),
            ('坐标基准未确认',sum(q['unconfirmed_coordinate_system_events'] for q in qc),'不能声称已可靠对齐雷达或地图。'),
            ('型深缺失',sum(q['missing_fields']['moulded_depth_m'] for q in qc),'不把船高或零值当成型深。')]
    quality_body += '<h3>融合条件与缺口</h3>'+table(['条件','观测数','占有效观测比例','解释'],[[a,num(v),f'{v/observed*100:.2f}%',b] for a,v,b in gaps])
    fields = [('ship_name','船名'),('length_m','登记船长'),('width_m','登记船宽'),('moulded_depth_m','型深'),('longitude','经度'),('latitude','纬度'),('sog_knots','航速'),('cog_deg','航迹向')]
    quality_body += '<h3>有效观测中的字段缺失</h3>'+table(['字段','ais_parsing_ori','REALTIME_TOPIC','合计'],[[label,num(qc[0]['missing_fields'][key]),num(qc[1]['missing_fields'][key]),num(sum(q['missing_fields'][key] for q in qc))] for key,label in fields])+'''
<p class="caption">分母是 267,525 条有效 AIS 观测，不含异常行。一条可同时缺多个字段，不可相加当成独立问题条数。艏向不可用：原解析侧 153,409 条，平台侧 35,892 条；艏向与航迹向分别处理。</p>
<p>原解析侧桥区 JSONL 只有 8,988 个不同 MMSI / last_updated 组合，多次接收可能带回同一来源位置。当前逐条留档，同时按字段来源时刻更新状态，不用接收新快照的时间伪装位置更新。</p>'''+link('reports/validation/ais-offline-parity.json','全量质量报告')+' · '+link('reports/validation/ais-supplement-audit.json','原始包核查')
    blocks.append(section('quality','05 / 真实数据规模与质量',quality_body))
    v = fusion['validation']
    validation = [
        ('真实数据回归',f'{v["real_data_tests"]} 项通过','只使用交付原件，覆盖协议、真实异常、迁移、导出契约与事务回滚等。'),
        ('全量新库与重导入',f'{num(total)} 条，通过','两版新库逐条比较；全量重导入去重后结果一致。'),
        ('融合前后业务结果',v['before_after_business_parity'],'原文、既有字段、状态和错误语义一致；新增传输元数据不伪装成旧版已有。'),
        ('五 Topic Kafka 集成',v['five_topic_recovery_and_dedup'],'独立临时集群验证提交恢复、重放去重、媒体与 AIS 隔离。'),
        ('两版质量 / 导出契约',v['python_rust_quality_parity']+' / '+v['contract_export_parity'],'版本包装和稳定来源 ID 相同，不取代上游业务事件标识。'),
        ('旧库迁移 / 事务回滚',v['existing_libraries_migration']+' / '+v['transaction_rollback_python_rust'],'旧数据不补造 key / headers，失败无半批状态写入。'),
        ('实际回放 key / headers',v['actual_replay_keys_and_empty_headers'],'覆盖 MMSI 回放 key 和空 headers；尚无非空现网 headers 原始样本。'),
        ('最近当前库只读比较',receipt['current_databases_result'],'核对时间 '+receipt['generated_at']+'；五 Topic 两版当前库逐条一致，两个 AIS 业务摘要与全量报告一致。')]
    blocks.append(section('verification','06 / 一致性与可靠性验证',table(['验证项','结果','覆盖范围'],validation)+'''
<div class="callout"><b>“结果一致”具体比较什么</b><p>原始 value 字节、来源位置、错误、全部规范化业务字段与最终 MMSI 状态；支持传输留档的两版另比较实际 key 和有序 headers。只排除执行时间造成的接收时钟差异和 JSON 对象键顺序，不使用数值容差。离线文件身份与行索引不冒充现网 Kafka offset。</p></div>'''+link('reports/dashboard/receipt.json','最近一次只读核对凭据')+' · '+link('reports/validation/ais-kafka-parity.json','AIS Kafka 集成')+' · '+link('reports/validation/site-protocols-integration.json','告警与媒体集成')))
    states = {'local':'本地已验证','pending':'待补数据 / 联调','partial':'部分实现，待标定','external':'后续模块待验收'}
    reqs = requirements(matched and full['result']=='PASS' and receipt['current_databases_result']=='PASS')
    rows = [[r['section'],escape(r['name']),f'<span class="status {"good" if r["state"]=="local" else "warn"}">{states[r["state"]]}</span>',escape(r['implemented'])+'<p class="caption">后续：'+escape(r['acceptance'])+'</p>'] for r in reqs]
    blocks.append(section('acceptance','07 / 对照 HTML：完成范围与剩余验收',
        '<p>按原始条款标明范围，不把接入层 PASS 等同于两个业务功能块全部交付。</p>'+table(['HTML 条款','要求','状态','当前实现与后续条件'],rows)+'''
<p>视频文件名是秒级系统段起始时间，OSD 来自相机时钟，需双重校时；告警保留毫秒，点云按 recv_ms 对齐。当前不依靠未校准时钟、未确认坐标或固定经验门限声称已正确关联。VTS 接入方式仍需明确。</p>'''+link('docs/requirements/常泰桥梁_功能需求说明.html','原始功能需求 HTML')))
    blocks.append(section('delivery','08 / 项目组织与复核入口','''
<pre>Kafka/
├── manage.py                   统一命令
├── src/python/bridge_kafka/     Python 当前实现
├── src/rust/                   Rust 当前实现
├── tools/validation/           真实数据一致性与恢复验证
├── tools/performance/          相同条件的吞吐测试
├── tools/inspection/           验收页与总结生成
├── configs/                    五个实际 Topic 的配置
├── data/                       原始包、原件、真实输入整理结果
├── docs/                       原始要求与工作说明
├── reports/                    结果与历史报告
├── references/                 师弟原始 ZIP（不重复保留解压副本）
├── var/                        当前库与本机 Kafka 数据
└── .runtime/                   运行与构建依赖</pre>
<pre>conda activate kafka
python manage.py summary                 # 根据已有报告更新总结
python manage.py dashboard --serve       # 只读核对当前库并提供验收页
python manage.py verify                  # 真实数据回归
python manage.py verify --full           # 全量新库与重导入核对
python manage.py integration             # 独立 Kafka 的五 Topic 恢复验证
python manage.py throughput --rounds 4 --warmup 5000</pre>
<p class="caption">总结生成不重新压测、不写业务库、不发布消息；交互验收页运行 dashboard 命令时再生成，不重复保留旧页面和截图。说明统一在根目录 README，本页集中保存成果、性能和质量。原始需求 HTML、交付包内说明和输出契约独立保留。压测和集成验证结束后清理临时库与集群。旧基线源码已删除，当前 throughput 默认对比现有两版，最新四方历史对照保留在原始报告中。</p>'''+link('README.md','项目使用与可视化验收入口')+' · '+link('docs/contracts/archive-record-v1.schema.json','留档导出契约')))
    erows = []
    for path,label,data,sha in evidence:
        expected = data.get('source_code_sha256')
        state = '与当前源码一致' if expected==code else '历史版本，仅作历史依据' if expected else '未记录源码指纹'
        erows.append([link(path,label),state,f'<code class="hash">{sha}</code>'])
    generated = datetime.now().astimezone().isoformat(timespec='seconds')
    match_text = '最新性能、全量核对、融合汇总和最近只读核对的源码指纹均与当前一致。' if matched else '部分最新报告与当前源码指纹不符，需复测后再作为当前验收依据。'
    blocks.append(section('sources','09 / 证据、版本与统计口径',
        f'<p>总结整理时间：{generated}。最新性能批次：{perf["date_CN"]}。质量取最新全量新库报告；最近当前库核对时间取验收页凭据。整理总结不表示刚刚重新运行所有测试。</p><p>当前 Python / Rust 源码 SHA-256：<code class="hash">{code}</code></p><p class="{"good" if matched else "warn"}">{match_text}</p>'+
        table(['原始证据','源码对应关系','文件 SHA-256'],erows)+
        '<p class="caption">HTML 主体自包含，可离线阅读、打印或单独转发。链接为项目相对路径：单独发送此 HTML 可以看全部正文，打开附带证据和源码则需保留相应项目目录。</p>'))
    metrics = ''.join(f'<div><span>{label}</span><strong>{value}</strong><small>{note}</small></div>' for label,value,note in [
        ('真实 AIS 全量核对',num(total),'JSONL + CSV 独有记录'),('协议有效 / 异常',num(valid)+' / '+num(invalid),'两者均留档，异常不补造'),
        ('Rust 当前吞吐',num(perf['channels'][0]['summary']['rust']['messages_per_second']),'原解析侧，条/秒中位数'),('实际 Topic','5','两种语言同流程，本地验证')])
    template = (Path(__file__).with_name('templates')/'summary.html').read_text()
    html = template.replace('{{DATE}}',escape(generated)).replace('{{METRICS}}',metrics).replace('{{CONTENT}}',''.join(blocks))
    assert '{{' not in html
    OUTPUT.write_text(html)
    print(OUTPUT)
    return OUTPUT


def main():
    generate()


if __name__ == '__main__':
    main()

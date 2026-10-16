//! Bounded durable batches; one application worker, same native libraries as Python.
#[macro_use]
mod profiling;
mod model;
mod diagnostics;
mod store;
mod site;
mod continuous;

use anyhow::{bail, Context as AnyhowContext, Result};
use clap::Parser;
use rdkafka::{config::ClientConfig, ClientContext, consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance}, message::Headers, Message, Offset, TopicPartitionList};
use serde_json::json;
use std::{collections::HashMap, io::{BufRead, BufReader}, path::PathBuf,
    sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}}, time::{Duration, Instant}};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value="127.0.0.1:9092")] bootstrap_servers: String,
    #[arg(long)] topic: Option<String>,
    #[arg(long, default_value="ais-rust-compare")] group: String,
    #[arg(long, default_value="var/ais-rust.sqlite3")] database: PathBuf,
    #[arg(long, default_value_t=0)] count: usize,
    #[arg(long, default_value_t=30)] idle_timeout: u64,
    #[arg(long, default_value_t=2048)] batch_size: usize,
    #[arg(long, default_value_t=50)] batch_max_wait_ms: u64,
    #[arg(long)] verbose_records: bool,
    #[arg(long)] jsonl: Option<PathBuf>,
    #[arg(long)] versions: bool,
    #[arg(long, value_parser=["standard","ct_ais","ct_alarm","ct_down_pic","ct_down_pic_plus"])] profile: Option<String>,
    #[arg(long, default_value="unknown", value_parser=["unknown","knots","mps","kmh"])] speed_unit: String,
    #[arg(long)] alarm_angle_is_cog: bool,
    #[arg(long, default_value="unknown", value_parser=["unknown","WGS84"])] coordinate_system: String,
    #[arg(long)] validate: bool,
    #[arg(long)] export_records: Option<PathBuf>,
    #[arg(long)] source_id: Option<String>,
    #[arg(long)] quality: bool,
}

fn log(message: &str) {
    eprintln!("{} INFO {}",chrono::Local::now().format("%Y-%m-%d %H:%M:%S,%3f"),message);
}

struct Writer {
    store: store::Store,
    pending: Vec<store::Record>,
    started: Option<Instant>,
    verbose: bool,
    failed: Option<String>,
    closing: bool,
}
impl Writer {
    fn add(&mut self, record: store::Record) {
        if self.pending.is_empty() { self.started = Some(Instant::now()); }
        self.pending.push(record);
    }
    fn flush(&mut self, consumer: Option<&BaseConsumer<Context>>, commit: bool) -> Result<()> {
        if self.pending.is_empty() { return Ok(()); }
        if let Some(error) = &self.failed { bail!("批次失败: {error}"); }
        let result = self.persist(consumer,commit);
        if let Err(error) = &result { self.failed = Some(error.to_string()); }
        result
    }
    fn persist(&mut self, consumer: Option<&BaseConsumer<Context>>, commit: bool) -> Result<()> {
        let saved = self.store.save_batch(&mut self.pending)?;
        if commit {
            let mut positions = HashMap::new();
            for record in &self.pending {
                positions.entry((record.topic.as_str(),record.partition)).and_modify(|offset: &mut i64|
                    *offset = (*offset).max(record.offset+1)).or_insert(record.offset+1);
            }
            let mut offsets = TopicPartitionList::new();
            for ((topic,partition),offset) in positions {
                offsets.add_partition_offset(topic,partition,Offset::Offset(offset))?;
            }
            consumer.unwrap().commit(&offsets,CommitMode::Sync)?;
        }
        if self.verbose {
            for (record,saved) in self.pending.iter().zip(&saved) {
                log(&format!("{} partition={} offset={} subject={} error={}", if *saved {"已保存"} else {"重读已去重"},
                    record.partition,record.offset,record.event.as_ref().and_then(|v| v["mmsi"].as_str().or(v["protocol"].as_str())).unwrap_or("-"),
                    record.error.as_deref().unwrap_or("-")));
            }
        } else {
            log(&format!("批次完成 records={} inserted={} invalid={} offsets_committed={commit}",
                self.pending.len(),saved.iter().filter(|v| **v).count(),self.pending.iter().filter(|r| r.error.is_some()).count()));
        }
        self.pending.clear();
        self.started = None;
        Ok(())
    }
}
struct Context { writer: Arc<Mutex<Writer>> }
impl ClientContext for Context {}
impl ConsumerContext for Context {
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if matches!(rebalance,Rebalance::Revoke(_)) {
            let mut writer = self.writer.lock().unwrap();
            if !writer.closing && writer.failed.is_none() {
                if let Err(error) = writer.flush(Some(consumer), !consumer.assignment_lost()) {
                    log(&format!("分区撤销时批次失败: {error}"));
                }
            }
        }
    }
}
static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn stop(_: libc::c_int) { STOP.store(true,Ordering::Relaxed); }

fn main() -> Result<()> {
    let args = Args::parse();
    if args.versions {
        println!("{}",json!({"librdkafka":rdkafka::util::get_rdkafka_version().1,"sqlite":rusqlite::version()}));
        return Ok(());
    }
    if args.quality {
        let store = store::Store::open(&args.database)?;
        println!("{}",serde_json::to_string_pretty(&store.quality_report()?)?);
        if let Some(path) = &args.export_records { store.export_records(path)?; }
        return Ok(());
    }
    let topic = match &args.topic {
        Some(value) if !value.trim().is_empty() => value.clone(),
        _ if args.jsonl.is_some() => "offline-file".to_string(),
        _ => bail!("必须显式提供 --topic；普通 AIS Topic 尚未知，不预设名称"),
    };
    let input = site::Input { profile: args.profile.clone().unwrap_or_else(|| site::Input::topic_profile(&topic).into()),
        speed_unit:args.speed_unit.clone(),angle_is_cog:args.alarm_angle_is_cog,coordinate_system:args.coordinate_system.clone(),
        source_topic: if ["ais_parsing_ori","REALTIME_TOPIC"].contains(&topic.as_str()) {Some(topic.clone())} else {None} };
    if args.batch_size == 0 || args.batch_size > 10000 || args.batch_max_wait_ms == 0 {
        bail!("batch_size 须为 1..10000，batch_max_wait_ms 须大于 0");
    }
    if args.validate {
        let path = args.jsonl.as_ref().context("--validate 必须同时提供 --jsonl")?;
        let mut invalid = 0;
        for (index,line) in BufReader::new(std::fs::File::open(path)?).split(b'\n').enumerate() {
            let mut raw = line?;
            if index == 0 && raw.starts_with(&[0xef,0xbb,0xbf]) { raw.drain(..3); }
            while raw.last().is_some_and(|b| *b == b'\r') { raw.pop(); }
            if raw.iter().all(u8::is_ascii_whitespace) { continue; }
            match input.normalize(Some(&raw)) {
                Ok(message) => println!("{}",json!({"line":index+1,"message":message})),
                Err(error) => { invalid+=1; println!("{}",json!({"line":index+1,"error":error.to_string()})); }
            }
        }
        #[cfg(feature = "profile")]
        profiling::report();
        if invalid > 0 { bail!("{invalid} 条消息无效"); }
        return Ok(());
    }
    let writer = Arc::new(Mutex::new(Writer {store:store::Store::open(&args.database)?,pending:Vec::new(),
        started:None,verbose:args.verbose_records,failed:None,closing:false}));
    if let Some(path) = args.jsonl {
        let digest = std::process::Command::new("sha256sum").arg("--").arg(&path).output()
            .context("离线来源校验需要 sha256sum")?;
        if !digest.status.success() { bail!("无法计算源文件 SHA-256"); }
        let output = String::from_utf8(digest.stdout)?;
        let hash = output.split_whitespace().next().unwrap_or("").trim_start_matches('\\');
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) { bail!("SHA-256 输出无效"); }
        let source_id = args.source_id.unwrap_or_else(|| path.file_name().unwrap_or_default().to_string_lossy().into_owned());
        let offline_topic = format!("offline:{source_id}:{hash}");
        let mut total = 0;
        let mut invalid = 0;
        let mut writer = writer.lock().unwrap();
        for (offset,line) in BufReader::new(std::fs::File::open(path)?).split(b'\n').enumerate() {
            let mut raw = line?;
            if offset == 0 && raw.starts_with(&[0xef,0xbb,0xbf]) { raw.drain(..3); }
            while raw.last().is_some_and(|b| *b == b'\r') { raw.pop(); }
            if raw.iter().all(u8::is_ascii_whitespace) { continue; }
            let record = store::Record::with_input(&offline_topic,0,offset as i64,Some(raw),None,&input);
            if record.error.is_some() { invalid += 1; }
            writer.add(record);
            if writer.pending.len() >= args.batch_size { writer.flush(None,false)?; }
            total += 1;
        }
        writer.flush(None,false)?;
        if let Some(path) = &args.export_records { writer.store.export_records(path)?; }
        println!("{}",json!({"processed":total,"invalid":invalid,"mode":"offline-parity","source":offline_topic}));
        #[cfg(feature = "profile")]
        profiling::report();
        if invalid > 0 { bail!("{invalid} 条无效消息已留档"); }
        return Ok(());
    }
    unsafe {
        libc::signal(libc::SIGINT,stop as *const () as libc::sighandler_t);
        libc::signal(libc::SIGTERM,stop as *const () as libc::sighandler_t);
    }
    let mut client_config = ClientConfig::new();
    client_config.set("bootstrap.servers",&args.bootstrap_servers)
        .set("security.protocol",std::env::var("AIS_SECURITY_PROTOCOL").unwrap_or_else(|_| "PLAINTEXT".into()))
        .set("group.id",&args.group)
        .set("auto.offset.reset","earliest")
        .set("enable.auto.commit","false")
        .set("enable.auto.offset.store","false")
        .set("allow.auto.create.topics","false");
    for (env,key) in [("AIS_SASL_MECHANISM","sasl.mechanism"),("AIS_SASL_USERNAME","sasl.username"),
                      ("AIS_SASL_PASSWORD","sasl.password"),("AIS_SSL_CA_LOCATION","ssl.ca.location")] {
        if let Ok(value) = std::env::var(env) { if !value.is_empty() { client_config.set(key,value); } }
    }
    let consumer: BaseConsumer<Context> = client_config.create_with_context(Context {writer:writer.clone()})?;
    consumer.subscribe(&[&topic])?;
    log(&format!("开始接收 topic={} group={} database={} batch_size={} batch_max_wait_ms={}",
        topic,args.group,args.database.display(),args.batch_size,args.batch_max_wait_ms));
    let started = Instant::now();
    let max_wait = Duration::from_millis(args.batch_max_wait_ms);
    let mut last_message = Instant::now();
    let mut count = 0;
    let mut active_started = None;
    let mut active_seconds = 0.0;
    let result = (|| -> Result<()> {
        while (args.count == 0 || count < args.count) && !STOP.load(Ordering::Relaxed) {
            let wait = {
                let writer = writer.lock().unwrap();
                if let Some(error) = &writer.failed { bail!("批次失败: {error}"); }
                writer.started.map(|t| max_wait.saturating_sub(t.elapsed())).unwrap_or(max_wait)
            };
            if let Some(message) = consumer.poll(wait) {
                let message = message?;
                active_started.get_or_insert_with(Instant::now);
                let mut record = store::Record::with_input(message.topic(),message.partition(),message.offset(),
                    message.payload().map(<[u8]>::to_vec),message.timestamp().to_millis(),&input);
                record.key = message.key().map(<[u8]>::to_vec);
                record.headers = Some(message.headers().map(|headers| headers.iter().map(|header|
                    (header.key.to_string(),header.value.map(<[u8]>::to_vec))).collect()).unwrap_or_default());
                writer.lock().unwrap().add(record);
                count += 1;
                last_message = Instant::now();
            } else if args.idle_timeout > 0 && last_message.elapsed() >= Duration::from_secs(args.idle_timeout) {
                break;
            }
            let mut writer = writer.lock().unwrap();
            if writer.pending.len() >= args.batch_size || writer.started.is_some_and(|t| t.elapsed() >= max_wait) {
                writer.flush(Some(&consumer),true)?;
            }
        }
        writer.lock().unwrap().flush(Some(&consumer),true)?;
        active_seconds = active_started.map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0);
        Ok(())
    })();
    writer.lock().unwrap().closing = true;
    drop(consumer);
    #[cfg(feature = "profile")]
    profiling::report();
    result?;
    if let Some(path) = &args.export_records { writer.lock().unwrap().store.export_records(path)?; }
    if !STOP.load(Ordering::Relaxed) && args.count > 0 && count < args.count { bail!("只接收 {count} 条，未达到要求的 {} 条",args.count); }
    println!("{}",json!({"processed":count,"elapsed_seconds":started.elapsed().as_secs_f64(),"active_seconds":active_seconds,"mode":"kafka-batch"}));
    Ok(())
}

use anyhow::Result;
use base64::{engine::general_purpose::STANDARD, Engine};
use rusqlite::{params, Connection, OpenFlags};
use serde_json::{json, Value};
use std::{collections::HashMap, path::Path, time::Duration};
use crate::model::{Snapshot, utc_now, DYNAMIC, STATIC};
use crate::diagnostics::{RECORD_SCHEMA, record_id};

fn dump_into(bytes: &mut Vec<u8>, value: &Value) -> Result<()> {
    use serde::Serialize;
    bytes.clear();
    value.serialize(&mut serde_json::Serializer::new(bytes))?;
    Ok(())
}
fn encoded<'a>(buffer: &'a mut Vec<u8>, value: Option<&Value>) -> Result<Option<&'a str>> {
    match value {
        Some(value) => { dump_into(buffer,value)?; Ok(Some(std::str::from_utf8(buffer)?)) },
        None => Ok(None),
    }
}

pub struct Store { pub db: Connection }
impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE | OpenFlags::SQLITE_OPEN_FULL_MUTEX)?;
        db.busy_timeout(Duration::from_secs(5))?;
        db.set_prepared_statement_cache_capacity(128);
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS records (
                topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id INTEGER NOT NULL,
                received_at TEXT NOT NULL, kafka_timestamp_ms INTEGER,
                raw_base64 TEXT NOT NULL, status TEXT NOT NULL,
                error TEXT, normalized_json TEXT, raw_is_null INTEGER NOT NULL DEFAULT 0,
                kafka_key_base64 TEXT, kafka_headers_json TEXT,
                PRIMARY KEY(topic, partition_id, offset_id));
            CREATE TABLE IF NOT EXISTS latest_ship (
                mmsi TEXT PRIMARY KEY, dynamic_time TEXT, dynamic_json TEXT,
                static_time TEXT, static_json TEXT);")?;
        let columns: Vec<String> = db.prepare("PRAGMA table_info(records)")?
            .query_map([], |row| row.get(1))?.collect::<std::result::Result<_,_>>()?;
        if !columns.iter().any(|name| name == "raw_is_null") {
            db.execute("ALTER TABLE records ADD COLUMN raw_is_null INTEGER NOT NULL DEFAULT 0", [])?;
        }
        for name in ["kafka_key_base64","kafka_headers_json"] {
            if !columns.iter().any(|column| column == name) {
                db.execute(&format!("ALTER TABLE records ADD COLUMN {name} TEXT"), [])?;
            }
        }
        Ok(Self{db})
    }

    pub fn save_batch(&mut self, records: &mut [Record]) -> Result<Vec<bool>> {
        let transaction = self.db.transaction()?;
        let mut saved = Vec::with_capacity(records.len());
        // Finish normalization metadata before retaining immutable references to events.
        for record in records.iter_mut() {
            if let Some(data) = &mut record.event {
                let items: &mut [Value] = if data["message_type"] == "batch" {
                    data["events"].as_array_mut().unwrap().as_mut_slice()
                } else { std::slice::from_mut(data) };
                for item in items {
                    if item["message_type"] != "media_command" && DYNAMIC.iter().any(|name| item["payload"].get(*name).is_some()) {
                        item["quality"]["position_gate_rejections"] = json!([]);
                    }
                }
            }
        }
        {
        // Prepared statements stay checked out for the batch instead of re-entering the cache per row.
        let mut insert = transaction.prepare_cached("INSERT OR IGNORE INTO records
            (topic,partition_id,offset_id,received_at,kafka_timestamp_ms,raw_base64,status,error,normalized_json,raw_is_null,kafka_key_base64,kafka_headers_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")?;
        let mut ensure_ship = transaction.prepare_cached("INSERT OR IGNORE INTO latest_ship(mmsi) VALUES (?)")?;
        let mut read_ship = transaction.prepare_cached("SELECT static_json,dynamic_json FROM latest_ship WHERE mmsi=?")?;
        let mut write_ship = transaction.prepare_cached("UPDATE latest_ship SET static_time=?,static_json=?,dynamic_time=?,dynamic_json=? WHERE mmsi=?")?;
        let mut snapshots: HashMap<String,(Snapshot<'_>,Snapshot<'_>)> = HashMap::new();
        let mut normalized = Vec::with_capacity(2048);
        let mut raw_base64 = String::with_capacity(1024);
        let mut key_base64 = String::new();
        let mut header_bytes = Vec::new();
        for record in records.iter() {
            let serialized = timed!("normalized_serialize", encoded(&mut normalized,record.event.as_ref()))?;
            raw_base64.clear();
            STANDARD.encode_string(record.raw.as_deref().unwrap_or(&[]),&mut raw_base64);
            key_base64.clear();
            let key = record.key.as_ref().map(|bytes| {
                STANDARD.encode_string(bytes,&mut key_base64); key_base64.as_str()
            });
            let header_json = match &record.headers {
                None => None,
                Some(headers) if headers.is_empty() => Some("[]"),
                Some(headers) => {
                    let values: Vec<Value> = headers.iter().map(|(name,value)|
                        json!({"key":name,"value_base64":value.as_ref().map(|v| STANDARD.encode(v))})).collect();
                    dump_into(&mut header_bytes,&json!(values))?;
                    Some(std::str::from_utf8(&header_bytes)?)
                }
            };
            let inserted = timed!("raw_insert", insert.execute(params![record.topic,record.partition,record.offset,
                utc_now(),record.timestamp,raw_base64,
                if record.error.is_some(){"invalid"}else{"valid"},record.error,serialized,record.raw.is_none(),key,header_json]))?;
            saved.push(inserted != 0);
            if inserted == 0 { continue; }
            if let Some(event) = &record.event {
                let items: &[Value] = if event["message_type"] == "batch" {
                    event["events"].as_array().unwrap().as_slice()
                } else { std::slice::from_ref(event) };
                for event in items {
                    if event["message_type"] == "media_command" { continue; }
                    let mmsi = event["mmsi"].as_str().unwrap();
                    if !snapshots.contains_key(mmsi) {
                        timed!("snapshot_read", {
                        ensure_ship.execute([mmsi])?;
                        let (s,d): (Option<String>,Option<String>) = read_ship.query_row([mmsi],|row| Ok((row.get(0)?,row.get(1)?)))?;
                        snapshots.insert(mmsi.into(),(Snapshot::new(s.map(|s| serde_json::from_str(&s)).transpose()?,"static",&STATIC),
                            Snapshot::new(d.map(|s| serde_json::from_str(&s)).transpose()?,"dynamic",&DYNAMIC)));
                        });
                    }
                    let (s,d) = snapshots.get_mut(mmsi).unwrap();
                    timed!("snapshot_merge", {
                        if STATIC.iter().any(|name| event["payload"].get(*name).is_some()) { s.apply(event); }
                        if DYNAMIC.iter().any(|name| event["payload"].get(*name).is_some()) { d.apply(event); }
                    });
                }
            }
        }
        timed!("snapshot_write", {
            let mut static_json = Vec::with_capacity(8192);
            let mut dynamic_json = Vec::with_capacity(8192);
            for (mmsi,(s,d)) in snapshots {
                let s=s.finish(); let d=d.finish();
                write_ship.execute(params![s.as_ref().and_then(|v| v["event_time"].as_str()),encoded(&mut static_json,s.as_ref())?,
                    d.as_ref().and_then(|v| v["event_time"].as_str()),encoded(&mut dynamic_json,d.as_ref())?,mmsi])?;
            }
        });
        }
        timed!("transaction_commit", transaction.commit())?;
        Ok(saved)
    }

    pub fn quality_report(&self) -> Result<Value> { crate::diagnostics::quality_report(&self.db) }

    pub fn export_records(&self, path: &Path) -> Result<()> {
        let mut statement = self.db.prepare("SELECT topic,partition_id,offset_id,received_at,kafka_timestamp_ms,status,error,normalized_json,kafka_key_base64,kafka_headers_json
            FROM records ORDER BY received_at,topic,partition_id,offset_id")?;
        let rows = statement.query_map([], |r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?,r.get::<_,String>(3)?,
            r.get::<_,Option<i64>>(4)?,r.get::<_,String>(5)?,r.get::<_,Option<String>>(6)?,r.get::<_,Option<String>>(7)?,r.get::<_,Option<String>>(8)?,r.get::<_,Option<String>>(9)?)))?;
        let mut output = Vec::new();
        for row in rows {
            let (topic,partition,offset,received,timestamp,status,error,normalized,key,headers) = row?;
            let message = normalized.map(|s| serde_json::from_str::<Value>(&s)).transpose()?;
            let recorded = headers.is_some();
            let headers: Option<Value> = headers.map(|s| serde_json::from_str(&s)).transpose()?;
            output.push(json!({"schema_version":RECORD_SCHEMA,"record_id":record_id(&topic,partition,offset),"topic":topic,"partition":partition,"offset":offset,"received_at":received,
                "kafka_timestamp_ms":timestamp,"status":status,"error":error,"message":message,
                "kafka_metadata_recorded":recorded,"kafka_key_base64":key,"kafka_headers":headers}));
        }
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) { std::fs::create_dir_all(parent)?; }
        let temporary = path.with_extension("json.tmp");
        std::fs::write(&temporary,serde_json::to_vec_pretty(&output)?)?;
        std::fs::rename(temporary,path)?;
        Ok(())
    }
}

pub struct Record {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub raw: Option<Vec<u8>>,
    pub event: Option<Value>,
    pub error: Option<String>,
    pub timestamp: Option<i64>,
    pub key: Option<Vec<u8>>,
    pub headers: Option<Vec<(String,Option<Vec<u8>>)>>,
}
impl Record {
    pub fn with_input(topic: &str, partition: i32, offset: i64, raw: Option<Vec<u8>>, timestamp: Option<i64>, input: &crate::site::Input) -> Self {
        let (event,error) = match timed!("normalize", input.normalize(raw.as_deref())) {
            Ok(event) => (Some(event),None), Err(error) => (None,Some(error.to_string()))
        };
        Self {topic:topic.into(),partition,offset,raw,event,error,timestamp,key:None,headers:None}
    }
}

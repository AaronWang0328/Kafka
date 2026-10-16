//! Deterministic archive contracts and quality reporting; no invented source event/time.
use anyhow::Result;
use rusqlite::Connection;
use serde_json::{json, Value};
use crate::model::FIELDS;

pub const RECORD_SCHEMA: &str = "bridge-kafka.record.v1";
pub fn record_id(topic: &str, partition: i64, offset: i64) -> String {
    format!("v1:{topic}:{partition}:{offset}")
}
fn increment(object: &mut Value, name: &str) {
    let count = object[name].as_u64().unwrap_or(0);
    object[name] = json!(count + 1);
}
pub fn quality_report(db: &Connection) -> Result<Value> {
    let mut statuses = json!({"valid":0,"invalid":0});
    let mut statement = db.prepare("SELECT status,COUNT(*) FROM records GROUP BY status")?;
    for row in statement.query_map([], |r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?)))? {
        let (status,count) = row?; statuses[status] = json!(count);
    }
    let records: i64 = db.query_row("SELECT COUNT(*) FROM records", [], |r| r.get(0))?;
    let ships: i64 = db.query_row("SELECT COUNT(*) FROM latest_ship", [], |r| r.get(0))?;
    let metadata: i64 = db.query_row("SELECT COUNT(*) FROM records WHERE kafka_headers_json IS NOT NULL", [], |r| r.get(0))?;
    let mut missing = json!({});
    for name in FIELDS { missing[name] = json!(0); }
    let mut result = json!({"schema_version":"bridge-kafka.quality.v1","records":records,"statuses":statuses,"ships":ships,
        "transport_metadata_records":metadata,"ais_events":0,"media_commands":0,"known_ais_times":0,"unknown_ais_times":0,
        "unconfirmed_coordinate_system_events":0,"missing_fields":missing,"issues":{},"position_gate_rejections":{}});
    let mut statement = db.prepare("SELECT normalized_json FROM records WHERE normalized_json IS NOT NULL")?;
    for row in statement.query_map([], |r| r.get::<_,String>(0))? {
        let data: Value = serde_json::from_str(&row?)?;
        let events: &[Value] = if data["message_type"] == "batch" { data["events"].as_array().unwrap() }
                              else { std::slice::from_ref(&data) };
        for event in events {
            if event["message_type"] == "media_command" { increment(&mut result,"media_commands"); continue; }
            increment(&mut result,"ais_events");
            increment(&mut result,if event["event_time"].is_null() {"unknown_ais_times"} else {"known_ais_times"});
            if event["source"]["coordinate_system"] == "unknown" { increment(&mut result,"unconfirmed_coordinate_system_events"); }
            for name in FIELDS {
                if event["payload"][name].is_null() { increment(&mut result["missing_fields"],name); }
            }
            for name in ["issues","position_gate_rejections"] {
                if let Some(reasons) = event["quality"][name].as_array() {
                    for reason in reasons { increment(&mut result[name],reason.as_str().unwrap()); }
                }
            }
        }
    }
    let mut statement = db.prepare("SELECT topic,partition_id,offset_id,error FROM records WHERE status='invalid' ORDER BY topic,partition_id,offset_id LIMIT 10")?;
    let mut invalid = Vec::new();
    for row in statement.query_map([], |r| Ok((r.get::<_,String>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?,r.get::<_,Option<String>>(3)?)))? {
        let (topic,partition,offset,error) = row?;
        invalid.push(json!({"record_id":record_id(&topic,partition,offset),"topic":topic,"partition":partition,"offset":offset,"error":error}));
    }
    result["first_invalid_records"] = json!(invalid);
    Ok(result)
}

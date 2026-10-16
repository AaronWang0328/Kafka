//! On-site per-vessel exports. Source clocks stay distinct from platform timestamps.
use anyhow::{bail, Result};
use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime, SecondsFormat, TimeZone, Utc};
use serde_json::{json, Map, Value};

fn fail<T>(field: &str) -> Result<T> { bail!("ct_ais: invalid_{field}") }
fn iso(ms: i64) -> Result<String> {
    Ok(DateTime::<Utc>::from_timestamp_millis(ms).ok_or_else(|| anyhow::anyhow!("ct_ais: invalid_last_updated"))?
       .to_rfc3339_opts(SecondsFormat::Millis, false))
}
fn numeric(row: &Value, key: &str, low: f64, high: Option<f64>, optional: bool, csv: bool) -> Result<Option<f64>> {
    let value = &row[key];
    if value.is_null() && optional { return Ok(None); }
    let number = if csv { value.as_str().and_then(|s| s.parse::<f64>().ok()) } else { value.as_f64() };
    match number {
        Some(n) if n.is_finite() && n >= low && high.is_none_or(|h| n <= h) => Ok(Some(n)),
        _ => fail(key),
    }
}

pub fn normalize(data: &Value, expected: Option<&str>, coordinate: &str) -> Result<Value> {
    if !data.is_object() { return fail("object"); }
    let topic = data["topic"].as_str().unwrap_or("");
    if !["ais_parsing_ori", "REALTIME_TOPIC"].contains(&topic) || expected.is_some_and(|s| s != topic) { return fail("topic"); }
    let csv = data["format"] == "ct_ais_track_csv";
    let row = if csv { &data["csv_row"] } else { data };
    if !row.is_object() || row["topic"].as_str() != Some(topic) { return fail("object"); }
    let mmsi = match &row["mmsi"] {
        Value::String(s) => s.clone(),
        Value::Number(n) if n.is_u64() => n.to_string(),
        _ => return fail("mmsi"),
    };
    if mmsi.len() != 9 || !mmsi.bytes().all(|c| c.is_ascii_digit()) { return fail("mmsi"); }
    let recv = row["recv"].as_str().unwrap_or("");
    if recv.len() != 23 || !recv.bytes().enumerate().all(|(i,b)| match i {
        4|7 => b == b'-', 10 => b == b' ', 13|16 => b == b':', 19 => b == b'.', _ => b.is_ascii_digit()
    }) { return fail("recv"); }
    let naive = NaiveDateTime::parse_from_str(recv, "%Y-%m-%d %H:%M:%S%.3f")
        .map_err(|_| anyhow::anyhow!("ct_ais: invalid_recv"))?;
    if !(1..=9999).contains(&naive.year()) || naive.and_utc().timestamp_subsec_nanos() >= 1_000_000_000 { return fail("recv"); }
    let recv_ms = FixedOffset::east_opt(8*3600).unwrap().from_local_datetime(&naive).single().unwrap().timestamp_millis();
    let mut issues: Vec<&str> = Vec::new();
    let name = match &row["name"] {
        Value::Null => None,
        Value::String(s) => Some(s.trim()).filter(|s| !s.is_empty()),
        _ => return fail("name"),
    };
    if name.is_none() { issues.push("unknown_ship_name"); }
    let mut length = numeric(row,"ship_length",0.0,None,true,csv)?;
    let mut width = numeric(row,"ship_width",0.0,None,true,csv)?;
    if length.is_none_or(|n| n == 0.0) { length=None; issues.push("unknown_length_m"); }
    if width.is_none_or(|n| n == 0.0) { width=None; issues.push("unknown_width_m"); }
    let mut course = numeric(row,if csv {"course_deg"} else {"course"},0.0,None,true,csv)?;
    if course.is_none_or(|n| n >= 360.0) { course=None; issues.push("unavailable_course"); }
    let mut heading = if csv {None} else {numeric(row,"heading",0.0,None,true,false)?};
    if heading.is_none_or(|n| n >= 360.0) { heading=None; issues.push("unavailable_heading"); }
    let unit = if topic == "ais_parsing_ori" {"seconds"} else {"milliseconds"};
    let updated_ms = if csv || row["last_updated"].is_null() { None } else {
        let Some(value) = row["last_updated"].as_f64() else { return fail("last_updated"); };
        let scaled = value * if unit == "seconds" {1000.0} else {1.0};
        if !scaled.is_finite() || !(0.0..=253402300799999.0).contains(&scaled) { return fail("last_updated"); }
        Some(scaled.floor() as i64)
    };
    let event_time = if topic == "ais_parsing_ori" { updated_ms.map(iso).transpose()? } else { None };
    if event_time.is_none() { issues.push("unconfirmed_position_time"); }
    if coordinate == "unknown" { issues.push("unconfirmed_coordinate_system"); }
    let fields = json!({"ship_name":name,"length_m":length,"width_m":width,
        "longitude":numeric(row,"lon",-180.0,Some(180.0),false,csv)?,
        "latitude":numeric(row,"lat",-90.0,Some(90.0),false,csv)?,
        "sog_knots":numeric(row,if csv {"speed_kn"} else {"speed"},0.0,None,true,csv)?,"cog_deg":course});
    let raw: Map<String,Value> = ["course","heading","last_updated"].iter()
        .filter_map(|k| row.get(*k).map(|v| ((*k).into(),v.clone()))).collect();
    let mut source = json!({"kind":if csv {"ais_track_csv"} else {"ais_export"},"topic":topic,
        "time_basis":if csv {"receive_only"} else if topic == "ais_parsing_ori" {"ais_last_updated"} else {"platform_createDate"},
        "received_time":iso(recv_ms)?,"observation_time":iso(updated_ms.unwrap_or(recv_ms))?,
        "last_updated_time":updated_ms.map(iso).transpose()?,"last_updated_unit":if csv {None} else {Some(unit)},
        "coordinate_system":coordinate,"reported_update_age_seconds":updated_ms.map(|ms| (recv_ms-ms) as f64 / 1000.0),
        "heading_deg":heading,"raw_navigation":raw,
        "export_format":if csv {"csv_track_projection"} else {"supplement_vessel_projection"}});
    if csv {
        for key in ["source_file","source_line"] { if let Some(v) = data.get(key) { source[key]=v.clone(); } }
    }
    let missing: Vec<&str> = crate::model::FIELDS.iter().copied().filter(|k| fields[*k].is_null()).collect();
    Ok(json!({"mmsi":mmsi,"event_time":event_time,"message_type":"combined","payload":fields,
        "source":source,"quality":{"missing_fields":missing,"source_time_known":event_time.is_some(),"issues":issues}}))
}

use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};

pub const STATIC: [&str; 4] = ["ship_name", "length_m", "width_m", "moulded_depth_m"];
pub const DYNAMIC: [&str; 4] = ["longitude", "latitude", "sog_knots", "cog_deg"];
pub const FIELDS: [&str; 8] = ["ship_name", "length_m", "width_m", "moulded_depth_m", "longitude", "latitude", "sog_knots", "cog_deg"];

pub fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, false)
}

pub fn normalize(raw: Option<&[u8]>) -> Result<Value> {
    let data: Value = serde_json::from_slice(raw.context("空消息")?).context("无效 UTF-8 JSON")?;
    let root = data.as_object().context("消息必须是 JSON 对象")?;
    let payload = root.get("payload").unwrap_or(&data).as_object().context("payload 必须是对象")?;
    let mmsi = match root.get("mmsi") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) if n.is_u64() => n.to_string(),
        _ => bail!("mmsi 必须是 9 位数字字符串或整数"),
    };
    if mmsi.len() != 9 || !mmsi.bytes().all(|b| b.is_ascii_digit()) {
        bail!("mmsi 必须为 9 位数字");
    }
    let has_static = STATIC.iter().any(|name| payload.contains_key(*name));
    let has_dynamic = DYNAMIC.iter().any(|name| payload.contains_key(*name));
    if !has_static && !has_dynamic { bail!("未找到 AIS 业务字段"); }
    let kind = if has_static && has_dynamic { "combined" } else if has_static { "static" } else { "dynamic" };
    let event_time = match root.get("event_time") {
        None | Some(Value::Null) => Value::Null,
        Some(Value::String(s)) => json!(DateTime::parse_from_rfc3339(s)?
            .with_timezone(&Utc).to_rfc3339_opts(SecondsFormat::Millis, false)),
        _ => bail!("采集时间必须是带时区的字符串"),
    };
    let mut clean = Map::new();
    for name in FIELDS {
        let Some(value) = payload.get(name) else { continue };
        if value.is_null() { clean.insert(name.into(), Value::Null); continue; }
        if name == "ship_name" {
            let s = value.as_str().context("船名类型错误")?.trim();
            if s.is_empty() { bail!("船名为空"); }
            clean.insert(name.into(), json!(s));
            continue;
        }
        let n = value.as_f64().context("数值类型错误")?;
        if !n.is_finite() { bail!("数值必须有限"); }
        let valid = match name {
            "longitude" => (-180.0..=180.0).contains(&n),
            "latitude" => (-90.0..=90.0).contains(&n),
            "cog_deg" => (0.0..360.0).contains(&n),
            "sog_knots" => n >= 0.0,
            _ => n > 0.0,
        };
        if !valid { bail!("{name} 超出范围"); }
        clean.insert(name.into(), json!(n));
    }
    let expected: &[&str] = if kind == "combined" { &FIELDS } else if kind == "static" { &STATIC } else { &DYNAMIC };
    let missing: Vec<&str> = expected.iter().copied().filter(|name| clean.get(*name).is_none_or(Value::is_null)).collect();
    Ok(json!({"mmsi":mmsi,"event_time":event_time,"message_type":kind,"payload":clean,
              "quality":{"missing_fields":missing,"source_time_known":!event_time.is_null()}}))
}

/// Retain references to the winning field updates during one transaction.
/// Each final field/source is copied once, rather than cloning entire snapshots per message.
pub struct Snapshot<'a> {
    previous: Option<Value>,
    updates: [Option<&'a Value>; 4],
    identity: Option<&'a Value>,
    kind: &'static str,
    names: &'static [&'static str; 4],
    has_sources: bool,
}
impl<'a> Snapshot<'a> {
    pub fn new(previous: Option<Value>, kind: &'static str, names: &'static [&'static str; 4]) -> Self {
        let has_sources = previous.as_ref().is_some_and(|v| v.get("field_sources").is_some());
        Self {previous,updates:[None;4],identity:None,kind,names,has_sources}
    }
    pub fn apply(&mut self, event: &'a Value) {
        self.identity = Some(&event["mmsi"]);
        self.has_sources |= event.get("source").is_some();
        let current_time = event["event_time"].as_str();
        let current_source = event.get("source");
        for (i,name) in self.names.iter().enumerate() {
            if event["payload"].get(*name).is_none() { continue; }
            let (old_time,old_source) = if let Some(previous) = self.updates[i] {
                (previous["event_time"].as_str(),previous.get("source"))
            } else if let Some(previous) = &self.previous {
                let time = if let Some(times) = previous.get("field_times").and_then(Value::as_object) {
                    times.get(*name).and_then(Value::as_str)
                } else if previous["payload"].get(*name).is_some() { previous["event_time"].as_str() }
                else { None };
                (time,previous.get("field_sources").and_then(|v| v.get(*name)))
            } else { (None,None) };
            if old_time.is_some_and(|old| current_time.is_none_or(|new| new < old)) { continue; }
            if old_time.is_none() && current_time.is_none() {
                if old_source.and_then(|v| v["kind"].as_str()) == Some("alarm_snapshot")
                    && current_source.and_then(|v| v["kind"].as_str()) == Some("alarm_snapshot") {
                    if let Some(old) = old_source.and_then(|v| v["observation_time"].as_str()) {
                        if current_source.and_then(|v| v["observation_time"].as_str()).is_none_or(|new| new < old) { continue; }
                    }
                }
                if let (Some(old),Some(new)) = (old_source,current_source) {
                    if [Some("ais_export"),Some("ais_track_csv")].contains(&old["kind"].as_str())
                        && [Some("ais_export"),Some("ais_track_csv")].contains(&new["kind"].as_str())
                        && old["topic"] == new["topic"] {
                        if let (Some(a),Some(b)) = (old["observation_time"].as_str(),new["observation_time"].as_str()) {
                            if b < a { continue; }
                        }
                    }
                }
            }
            self.updates[i] = Some(event);
        }
    }
    pub fn finish(self) -> Option<Value> {
        let Some(identity) = self.identity else { return self.previous; };
        fn object(value: Option<Value>) -> Map<String,Value> {
            match value {Some(Value::Object(map)) => map,_ => Map::new()}
        }
        let mut previous = object(self.previous);
        let mut payload = object(previous.remove("payload"));
        let old_time = previous.remove("event_time").unwrap_or(Value::Null);
        let mut times = match previous.remove("field_times") {
            Some(Value::Object(map)) => map,
            _ => payload.keys().map(|name| (name.clone(),old_time.clone())).collect(),
        };
        let mut sources = object(previous.remove("field_sources"));
        for (name,update) in self.names.iter().zip(self.updates) {
            if let Some(event) = update {
                payload.insert((*name).into(),event["payload"][*name].clone());
                times.insert((*name).into(),event["event_time"].clone());
                if let Some(source) = event.get("source") { sources.insert((*name).into(),source.clone()); }
                else { sources.remove(*name); }
            }
        }
        let latest = times.values().filter_map(Value::as_str).max().map(|s| Value::String(s.into())).unwrap_or(Value::Null);
        let mut result = Map::new();
        result.insert("mmsi".into(),identity.clone());
        result.insert("message_type".into(),Value::String(self.kind.into()));
        result.insert("event_time".into(),latest);
        result.insert("payload".into(),Value::Object(payload));
        result.insert("field_times".into(),Value::Object(times));
        if self.has_sources { result.insert("field_sources".into(),Value::Object(sources)); }
        Some(Value::Object(result))
    }
}

//! Protocols verified against the three original on-site Kafka captures.
use anyhow::{bail, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Map, Value};

#[derive(Clone)]
pub struct Input {
    pub profile: String,
    pub speed_unit: String,
    pub angle_is_cog: bool,
    pub coordinate_system: String,
    pub source_topic: Option<String>,
}

impl Input {
    pub fn topic_profile(topic: &str) -> &'static str {
        match topic { "ABNORMAL_TOPIC" => "ct_alarm", "down_pic" => "ct_down_pic",
                      "down_pic_plus" => "ct_down_pic_plus", "ais_parsing_ori" | "REALTIME_TOPIC" => "ct_ais", _ => "standard" }
    }

    fn capture_topic(&self) -> Option<&str> {
        match self.profile.as_str() { "ct_alarm" => Some("ABNORMAL_TOPIC"), "ct_down_pic" => Some("down_pic"),
                                     "ct_down_pic_plus" => Some("down_pic_plus"), _ => None }
    }

    pub fn normalize(&self, raw: Option<&[u8]>) -> Result<Value> {
        let mut data: Value = serde_json::from_slice(raw.context("空消息")?).context("无效 UTF-8 JSON")?;
        if self.profile == "ct_ais" {
            return crate::continuous::normalize(&data, self.source_topic.as_deref(), &self.coordinate_system);
        }
        let mut capture = None;
        if let Some(expected) = self.capture_topic() {
            if data.is_object() && data.get("value").is_some() && data.get("topic").is_some() {
                if data["topic"].as_str() != Some(expected) { bail!("消费留档 Topic 与当前协议不符"); }
                let mut metadata = Map::new();
                for name in ["topic", "partition", "offset", "kafka_ts", "recv"] {
                    if let Some(value) = data.get(name) {
                        if ["partition", "offset", "kafka_ts"].contains(&name) && !value.is_null() && value.as_u64().is_none() {
                            bail!("消费留档 {name} 必须是非负整数或 null");
                        }
                        if name == "recv" && !value.is_string() { bail!("消费留档 recv 必须是字符串"); }
                        metadata.insert(name.into(), value.clone());
                    }
                }
                let inner = data["value"].as_str().context("消费留档 value 必须是 JSON 字符串")?;
                let parsed = serde_json::from_str(inner).context("value 不是有效 UTF-8 JSON")?;
                capture = Some(Value::Object(metadata));
                data = parsed;
            }
        }
        let mut result = if self.profile == "ct_down_pic" || self.profile == "ct_down_pic_plus" {
            media_command(&data, &self.profile)?
        } else {
            let items: Vec<&Value> = if let Some(array) = data.as_array() { array.iter().collect() } else { vec![&data] };
            if items.is_empty() || items.len() > 10000 { bail!("JSON 数组必须含 1–10000 个船舶对象"); }
            let mut events = Vec::with_capacity(items.len());
            for item in items {
                events.push(if self.profile == "ct_alarm" { self.alarm(item)? }
                    else { crate::model::normalize(Some(&serde_json::to_vec(item)?))? });
            }
            if events.len() == 1 { events.remove(0) } else { json!({"message_type":"batch", "events":events}) }
        };
        if let Some(capture) = capture { result["capture"] = capture; }
        Ok(result)
    }

    fn alarm(&self, data: &Value) -> Result<Value> {
        data.as_object().context("告警必须是对象")?;
        let warn_type = data["warnType"].as_u64().context("warnType 必须是整数")?;
        if !(1..=8).contains(&warn_type) { bail!("warnType 必须是 1–8 的整数"); }
        let timestamp = data["warnDate"].as_f64().context("告警必须提供 warnDate 毫秒时间戳")?;
        if !timestamp.is_finite() || timestamp < 0.0 || timestamp > i64::MAX as f64 { bail!("warnDate 无效"); }
        let observed = DateTime::from_timestamp_millis(timestamp as i64).context("warnDate 无效")?
            .with_timezone(&Utc).to_rfc3339_opts(SecondsFormat::Millis, false);
        let mut alarm = json!({"warn_type":warn_type,"warn_time":observed});
        for (from,to) in [("eventId","event_id"),("warnInfo","info"),("warnGrade","grade"),("warnAction","action")] {
            if let Some(v) = data.get(from) { alarm[to] = json!(text(v, from)?); }
        }
        for (from,to) in [("warnImageUrl","image_url"),("warnVideoUrl","video_url")] {
            if let Some(v) = data.get(from).filter(|v| !v.is_null() && **v != json!("")) {
                alarm[to] = json!(public_uri(v, &["http","https"])?.0);
            }
        }
        let mut mapped = Map::new();
        for (to,from) in [("mmsi","mmsi"),("ship_name","name"),("length_m","shipLength"),
                          ("width_m","shipWidth"),("longitude","longitude"),("latitude","latitude")] {
            if let Some(v) = data.get(from) {
                mapped.insert(to.into(), if ["length_m","width_m"].contains(&to) && v.as_f64() == Some(0.0) {
                    Value::Null
                } else { v.clone() });
            }
        }
        if self.speed_unit != "unknown" {
            if let Some(v) = data.get("shipSpeed") {
                if v.is_null() { mapped.insert("sog_knots".into(), Value::Null); }
                else {
                    let speed = finite(v, "shipSpeed", 0.0, f64::MAX)?;
                    let factor = match self.speed_unit.as_str() { "mps" => 3600.0 / 1852.0, "kmh" => 1.0 / 1.852, _ => 1.0 };
                    mapped.insert("sog_knots".into(), json!(speed * factor));
                }
            }
        }
        if self.angle_is_cog {
            if let Some(v) = data.get("shipAngle") { mapped.insert("cog_deg".into(), v.clone()); }
        }
        let mut result = crate::model::normalize(Some(&serde_json::to_vec(&mapped)?))?;
        let raw_navigation: Map<String,Value> = ["shipSpeed","shipAngle","shipDirection","shipHeight"].iter()
            .filter_map(|name| data.get(*name).map(|v| ((*name).into(),v.clone()))).collect();
        result["source"] = json!({"kind":"alarm_snapshot","observation_time":observed,"time_basis":"alarm",
            "coordinate_system":self.coordinate_system,"event_id":data.get("eventId").cloned().unwrap_or(Value::Null),
            "warn_type":warn_type,"raw_navigation":raw_navigation});
        result["alarm"] = alarm;
        Ok(result)
    }
}

fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    let s = value.as_str().with_context(|| format!("{name} 必须是字符串"))?;
    if s.trim().is_empty() { bail!("{name} 不能为空"); }
    Ok(s)
}

fn path(value: &Value, name: &str) -> Result<String> {
    let s = text(value,name)?;
    if s.starts_with('/') || s.contains('\\') || s.split('/').any(|p| p == "..") || s.chars().any(|c| (c as u32) < 32) {
        bail!("{name} 必须是有效的相对媒体路径");
    }
    Ok(s.into())
}

fn finite(value: &Value, name: &str, low: f64, high: f64) -> Result<f64> {
    let n = value.as_f64().with_context(|| format!("{name} 必须是数值"))?;
    if !n.is_finite() || n < low || n > high { bail!("{name} 超出范围"); }
    Ok(n)
}

fn public_uri(value: &Value, schemes: &[&str]) -> Result<(String,bool)> {
    let s = text(value,"媒体地址")?;
    if s.chars().any(char::is_whitespace) { bail!("媒体地址无效"); }
    let (scheme,rest) = s.split_once("://").context("媒体地址无效")?;
    let scheme = scheme.to_ascii_lowercase();
    if !schemes.contains(&scheme.as_str()) { bail!("媒体地址协议无效"); }
    let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let authority = &rest[..end];
    let credentials = authority.contains('@');
    let authority = authority.rsplit('@').next().unwrap_or("");
    let (host,port) = if authority.starts_with('[') {
        let close = authority.find(']').context("媒体地址无效")?;
        if close == 1 { bail!("媒体地址无效"); }
        (&authority[..=close], &authority[close+1..])
    } else if let Some((host,port)) = authority.rsplit_once(':') {
        if host.contains(':') { bail!("媒体地址无效"); }
        (host,&authority[host.len()..host.len()+port.len()+1])
    } else { (authority,"") };
    if host.is_empty() { bail!("媒体地址无效"); }
    let normalized_port = if port.is_empty() { String::new() }
        else { format!(":{}", port.strip_prefix(':').context("媒体端口无效")?.parse::<u16>().context("媒体端口无效")?) };
    Ok((format!("{scheme}://{}{normalized_port}{}",host.to_lowercase(),&rest[end..]),credentials))
}

fn media_command(data: &Value, profile: &str) -> Result<Value> {
    let (protocol,payload) = if profile == "ct_down_pic" {
        let s = text(data,"down_pic value")?;
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 4 { bail!("down_pic 必须包含四段"); }
        let lon = parts[0].trim().parse::<f64>().context("经度无效")?;
        let lat = parts[1].trim().parse::<f64>().context("纬度无效")?;
        if !lon.is_finite() || !lat.is_finite() || !(-180.0..=180.0).contains(&lon) || !(-90.0..=90.0).contains(&lat) { bail!("经纬度超出范围"); }
        ("down_pic",json!({"longitude":lon,"latitude":lat,"coordinate_system":"unknown",
            "picture_path":path(&json!(parts[2]),"picture_path")?,"video_path":path(&json!(parts[3]),"video_path")?}))
    } else {
        let parts = data.as_array().context("down_pic_plus 必须是四元素数组")?;
        if parts.len() != 4 { bail!("down_pic_plus 必须是四元素数组"); }
        let (uri,credentials) = public_uri(&parts[0], &["rtsp","rtsps"])?;
        let option = parts[3].as_bool().context("第四项必须是布尔值")?;
        ("down_pic_plus",json!({"camera_uri":uri,"credentials_present":credentials,"picture_path":path(&parts[1],"picture_path")?,
            "video_path":path(&parts[2],"video_path")?,"option":option,"option_semantics":"unconfirmed"}))
    };
    Ok(json!({"message_type":"media_command","protocol":protocol,"event_time":null,"payload":payload,
        "quality":{"source_time_known":false}}))
}

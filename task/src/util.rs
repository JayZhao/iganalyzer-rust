use chrono::{DateTime, Utc, FixedOffset, SecondsFormat};

pub fn utc_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
}


pub fn parse_time(s: &str) -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339(s).unwrap()
}

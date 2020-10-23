use base64;
use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use rand::random;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use serde_json::error::Error;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Failure {
    pub retry_count: i32,
    pub failed_at: String,
    #[serde(skip)]
    pub next_at: String,
    #[serde(skip)]
    pub err_msg: String,
    #[serde(skip)]
    pub err_type: String,
    #[serde(skip)]
    pub backtrace: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Job {
    pub jid: String,
    pub queue: String,
    pub job_type: String,
    pub args: Option<Vec<String>>,

    result: Option<String>,
    #[serde(skip)]
    pub created_at: Option<String>,
    #[serde(skip)]
    pub enqueued_at: Option<String>,
    #[serde(skip)]
    pub at: Option<String>,
    #[serde(skip)]
    pub reserve_for: Option<i32>,
    #[serde(skip)]
    pub retry: Option<i32>,
    #[serde(skip)]
    pub backtrace: Option<i32>,
    #[serde(skip)]
    pub failure: Option<Failure>,
    #[serde(skip)]
    pub expired_at: Option<i32>,
    #[serde(skip)]
    pub unique_for: Option<i32>,
    #[serde(skip)]
    pub unique_until: Option<i32>,
}

impl Default for Job {
    fn default() -> Job {
        Job {
            job_type: "none".into(),
            queue: "default".into(),
            args: None,
            jid: Self::random_jid(),
            result: None,
            created_at: Some(Utc::now().to_rfc3339()),
            enqueued_at: None,
            at: None,
            reserve_for: None,
            retry: Some(0),
            backtrace: None,
            failure: None,
            expired_at: None,
            unique_for: None,
            unique_until: None,
        }
    }
}

impl Job {
    pub fn new(job_type: &str, args: Vec<String>) -> Job {
        Job {
            job_type: job_type.into(),
            args: Some(args),
            created_at: Some(Utc::now().to_rfc3339()),
            ..Default::default()
        }
    }

    pub fn set_enqueued_at(&mut self, timestamp: String) {
        self.enqueued_at = Some(timestamp);
    }

    pub fn random_jid() -> String {
        let mut rng = thread_rng();
        let buf = rng.gen::<[u8; 12]>();

        base64::encode(&buf)
    }

    pub fn next_try(&self) -> Option<DateTime<Utc>> {
        if let Some(failure) = &self.failure {
            let count = failure.retry_count as i64;
            let rand = random::<i64>() % 30;
            let sec = (count * count * count * count) + 15 + rand * (count + 1);

            return Some(Utc::now() + Duration::seconds(sec));
        }

        None
    }

    pub fn encode(&self) -> Result<Bytes, Error> {
        Ok(Bytes::copy_from_slice(
            serde_json::to_string(self)?.as_bytes(),
        ))
    }

    pub fn decode(buf: &[u8]) -> Result<Job, Error> {
        Ok(serde_json::from_str(&String::from_utf8_lossy(buf))?)
    }
}

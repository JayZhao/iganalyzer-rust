use std::collections::HashMap;
use chrono::Utc;
use rand::{Rng, thread_rng};
use base64;
use bytes::Bytes;

#[derive(Debug)]
pub struct Failure {
    retry_count: i32,
    failed_at: String,
    next_at: String,
    err_msg: String,
    err_type: String,
    backtrace: Vec<String>
}

#[derive(Debug)]
pub struct Job {
    pub jid: String,
    queue: String,
    job_type: String,
    args: Option<Bytes>,
    content: Option<Bytes>,
    result: Option<Bytes>,

    created_at: Option<String>,
    enqueued_at: Option<String>,
    pub at: Option<String>,
    reverse_for: Option<i32>,
    retry: Option<i32>,
    backtrace: Option<i32>,
    failure: Option<Failure>,
    expired_at: Option<i32>,
    unique_for: Option<i32>,
    unique_until: Option<i32>
}


impl Default for Job {
    fn default() -> Job {
        Job {
            job_type: "none".into(),
            queue: "default".into(),
            args: None,
            jid: Self::random_jid(),
            content: None,
            result: None,
            created_at: Some(Utc::now().to_rfc3339()),
            enqueued_at: None,
            at: None,
            reverse_for: None,
            retry: Some(2),
            backtrace: None,
            failure: None,
            expired_at: None,
            unique_for: None,
            unique_until: None
        }
    }
}

impl Job {
    pub fn new(job_type: &str, args: Bytes, content: Bytes) -> Job {
        Job {
            job_type: job_type.into(),
            args: Some(args),
            content: Some(content),
            created_at: Some(Utc::now().to_rfc3339()),
            ..Default::default()
        }
    }

    pub fn set_enqueued_at(&mut self, timestamp: String) {
        self.enqueued_at = Some(timestamp);
    }

    fn random_jid() -> String {
        let mut rng = thread_rng();
        let buf = rng.gen::<[u8; 12]>();
        
        base64::encode(&buf)
    }

    pub fn encode(&self) -> Bytes {
        let buf = Bytes::new();

        buf
    }

    pub fn decode(buf: &[u8]) -> Job {

        Default::default()
    }
}

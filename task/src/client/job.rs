use std::collections::HashMap;
use chrono::Utc;
use rand::{Rng, thread_rng};
use base64;

pub struct Failure {
    retry_count: i32,
    failed_at: String,
    next_at: String,
    err_msg: String,
    err_type: String,
    backtrace: Vec<String>
}

pub struct Job<A, C> {
    jid: String,
    queue: String,
    job_type: String,
    args: Vec<A>,

    created_at: Option<String>,
    enqueued_at: Option<String>,
    at: Option<String>,
    reverse_for: Option<i32>,
    retry: Option<i32>,
    backtrace: Option<i32>,
    failure: Option<Failure>,
    custom: Option<HashMap<String, C>>
}

impl<A, C> Job<A, C> {
    fn new(job_type: &str, args: Vec<A>) -> Job<A, C> {
        Job {
            job_type: job_type.into(),
            queue: "default".into(),
            args,
            jid: Self::random_jid(),
            created_at: Some(Utc::now().to_rfc3339()),
            enqueued_at: None,
            at: None,
            reverse_for: None,
            retry: Some(2),
            backtrace: None,
            failure: None,
            custom: None,
        }
    }

    fn random_jid() -> String {
        let mut rng = thread_rng();
        let buf = rng.gen::<[u8; 12]>();
        
        base64::encode(&buf)
    }
}

use chrono::{DateTime, Utc};
use bytes::Bytes;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use crate::client::job::Job;
use crate::Result;

#[async_trait]
trait Lease {
    async fn relase(&self) -> Result<()>;
    async fn payload(&self) -> Result<Bytes>;
}

#[derive(Debug)]
struct SimpleLease {
    payload: Bytes,
    job: Job,
    released: bool
}

#[async_trait]
impl Lease for SimpleLease {
    async fn relase(&self) -> Result<()> {
        Ok(())
    }
    async fn payload(&self) -> Result<Bytes> {
        Ok(Bytes::new())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Reservation {
    pub job: Job,
    since: String,
    expiry: String,
    wid: String,
    
    #[serde(skip)]
    tsince: Option<DateTime<Utc>>,

    #[serde(skip)]
    texpiry: Option<DateTime<Utc>>,

    #[serde(skip)]
    extension: Option<DateTime<Utc>>,
}


impl Reservation {
    pub fn encode(&self) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(
            serde_json::to_string(self)?.as_bytes(),
        ))
    }
    
    pub fn decode(buf: &[u8]) -> Result<Reservation> {
        Ok(serde_json::from_str(&String::from_utf8_lossy(buf))?)
    }
}

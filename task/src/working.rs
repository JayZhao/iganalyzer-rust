use chrono::{DateTime, Utc};
use bytes::Bytes;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use crate::client::job::Job;
use crate::Result;

#[async_trait]
trait Lease<'a> {
    async fn relase(&self) -> Result<()>;
    async fn payload(&self) -> Result<Bytes>;
    async fn job(&'a self) -> Result<&'a Job>;
}

#[derive(Debug)]
struct SimpleLease<'a> {
    payload: Bytes,
    job: &'a Job,
    released: bool
}

#[async_trait]
impl<'a> Lease<'a> for SimpleLease<'a> {
    async fn relase(&self) -> Result<()> {
        Ok(())
    }
    async fn payload(&self) -> Result<Bytes> {
        Ok(Bytes::new())
    }
    async fn job(&'a self) -> Result<&'a Job> {
        Ok(self.job)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Reservation<'a> {
    job: Job,
    since: String,
    expiry: String,
    wid: String,
    
    #[serde(skip)]
    tsince: Option<DateTime<Utc>>,

    #[serde(skip)]
    texpiry: Option<DateTime<Utc>>,

    #[serde(skip)]
    extension: Option<DateTime<Utc>>,

    #[serde(skip)]
    lease: Option<SimpleLease<'a>>
}


impl<'a> Reservation<'a> {
    pub fn encode(&self) -> Result<Bytes> {
        Ok(Bytes::copy_from_slice(
            serde_json::to_string(self).unwrap().as_bytes(),
        ))
    }
    
    pub fn decode(buf: &[u8]) -> Result<Reservation<'a>> {
        Ok(serde_json::from_str(&String::from_utf8_lossy(buf)).unwrap())
    }
}

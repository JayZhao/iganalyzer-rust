use crate::types::RedisStoreError;
use bytes::Bytes;

use crate::client::job::Job;

pub trait SortedSet {
    fn name(&self) -> String;
    fn size(&self) -> u64;
    fn clear(&mut self) -> Result<(), RedisStoreError>;
    fn add(&mut self, job: Job) -> Result<(), RedisStoreError>;
}


pub trait Store {
    fn close(&self) -> Result<(), RedisStoreError>;
    fn retries(&self) -> Result<(), RedisStoreError>;
    fn scheduled(&self) -> Result<(), RedisStoreError>;
}

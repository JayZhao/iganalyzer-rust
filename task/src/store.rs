use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::RwLock;
use bytes::Bytes;

use crate::util;
use crate::types::RedisStoreError;
use crate::redis_client::RedisClient;
use crate::client::job::Job;

struct RedisQueue {
    name: String,
    done: bool,
    store: Weak<RwLock<RedisStoreInner>>
}

impl RedisQueue {
    pub fn new(name: String, store: Weak<RwLock<RedisStoreInner>>) -> RedisQueue {
        RedisQueue {name, done: false, store}
    }

    pub fn close(&mut self) {
        self.done = true;
    }

    pub fn name(&self) -> String {
        self.name.to_string()
    }

    pub async fn size(&self) -> Result<u64, RedisStoreError> {
        if let Some(store) = self.store.upgrade() {
            store.read().await.client.execute::<u32, String>("LLEN", Some(&vec![self.name()])).await?;
        }

        Ok(0)
    }

    pub async fn clear(&self) -> Result<(), RedisStoreError> {
        if let Some(store) = self.store.upgrade() {
            let args1 = vec![self.name()];
            let args2 = vec!["queues".into(), self.name()];
            store.read().await.client.execute::<u32, String>("UNLINK", Some(&args1)).await?;
            store.read().await.client.execute::<u32, String>("SREM", Some(&args2)).await?;
            store.write().await.queue_set.remove(&self.name);
        }

        Ok(())
    }

    pub async fn add(&mut self, mut job: Job) -> Result<(), RedisStoreError> {
        let enqueued_at = util::utc_now();
        job.set_enqueued_at(enqueued_at);
        Ok(())
    }

    pub async fn push(&mut self, payload: &[u8]) -> Result<(), RedisStoreError> {

        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name.as_bytes(), payload];

            store.read().await.client.execute::<u32, &[u8]>("LPUSH", Some(&args)).await?;
        }
        
        Ok(())
    }

    pub async fn pop(&mut self) -> Result<Option<Vec<u8>>, RedisStoreError> {

        if self.done {
            return Err(RedisStoreError::QueueEmpty(self.name.clone()))
        }
        
        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name()];
            return Ok(Some(store.read().await.client.execute::<Vec<u8>, String>("RPOP", Some(&args)).await?));
        }


        Ok(None)
    }

    pub async fn bpop(&mut self) -> Result<Option<Vec<u8>>, RedisStoreError> {
        if self.done {
            return Err(RedisStoreError::QueueEmpty(self.name.clone()))
        }
        
        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name(), "2".into()];
            return Ok(Some(store.read().await.client.execute::<Vec<u8>, String>("BRPOP", Some(&args)).await?));
        }

        Ok(None)
    }

    pub async fn delete(&mut self, vals: &[u8]) -> Result<(), RedisStoreError> {

        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name.as_bytes(), vals];
            return Ok(store.read().await.client.execute::<(), &[u8]>("LREM", Some(&args)).await?);
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct RedisSorted {
    name: String,
    store: Weak<RwLock<RedisStoreInner>>
}

impl RedisSorted {
    fn name(&self) -> String {
        self.name.clone()
    }

    pub async fn size(&self) -> Result<Option<usize>, RedisStoreError> {
        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name.clone()];

            return Ok(store.read().await.client.execute::<Option<usize>, String>("ZCARD", Some(&args)).await?);
        }

        Ok(None)
    }

    pub async fn clear(&self) -> Result<(), RedisStoreError> {

        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name.clone()];

            store.read().await.client.execute::<(), String>("UNLINK", Some(&args)).await?;
            
        }
                                                          
        Ok(())
    }

    pub async fn add(&self, job: Job) -> Result<(), RedisStoreError> {
        let at = job.at.clone();
        if at.is_none() || at == Some("".into()) {
            return Err(RedisStoreError::JobErr("at is empty".into()));
        }
        
        self.add_elem(&at.clone().unwrap(), job.encode()).await?;

        Ok(())
    }

    pub async fn add_elem(&self, timestamp: &str, payload: Bytes) -> Result<(), RedisStoreError> {
        let at = util::parse_time(&timestamp);
        
        let time_f = (at.timestamp_nanos() as f64 / 1000000000.0).to_string();

        let name = self.name().clone();
        let args = vec![name.as_bytes(), time_f.as_bytes(), payload.as_ref()];

        if let Some(store) = self.store.upgrade() {
            store.read().await.client.execute::<usize, &[u8]>("ZADD", Some(&args)).await?;
            
        }

        Ok(())
    }

    // timestmap|jid
    pub fn decompose(key: &str) -> Result<(f64, String), RedisStoreError> {
        let slice = key.split("|").collect::<Vec<&str>>();

        if slice.len() != 2 {
            return Err(RedisStoreError::KeyInvalid(key.into()));
        }
        let timestamp = slice[0];
        let time = util::parse_time(timestamp);

        Ok((time.timestamp_nanos() as f64 / 1000000000.0, slice[1].to_string()))
    }

    pub async fn get_score(&self, score: f64) -> Result<Option<Vec<Vec<u8>>>, RedisStoreError> {
        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name.clone(), score.to_string(), score.to_string()];            
            return Ok(store.read().await.client.execute::<Option<Vec<Vec<u8>>>, String>("ZRangeByScore", Some(&args)).await?);
        }

        Ok(None)
    }

    pub async fn get(&self, key: &str) -> Result<Option<SetEntry>, RedisStoreError> {
        
        if let Ok((timestamp, jid)) = RedisSorted::decompose(key) {
            let elems  = self.get_score(timestamp).await?;

            if let Some(elems) = elems {
                if elems.len() == 1 {
                    return Ok(Some(SetEntry::new(Bytes::copy_from_slice(key.as_bytes()), Bytes::copy_from_slice(elems[0].as_ref()), Job::decode(&elems[0]))));
                }

                if elems.len() > 1 {

                    for elem in elems {
                        let job = Job::decode(&elem);

                        if job.jid == jid {
                            return Ok(Some(SetEntry::new(Bytes::copy_from_slice(key.as_bytes()), Bytes::copy_from_slice(elem.as_ref()), job)));
                        }
                    }
                }
            }
        }
        Ok(None)
    }
}

#[derive(Debug)]
struct SetEntry {
    value: Bytes,
    key: Bytes,
    job: Job
}

impl SetEntry {
    pub fn new(key: Bytes, value: Bytes, job: Job) -> SetEntry {
        SetEntry {
            key, value, job
        }
    }
}

struct RedisStoreInner {
    name: String,
    queue_set: HashMap<String, RedisQueue>,
    scheduled: Option<RedisSorted>,
    retries: Option<RedisSorted>,
    dead: Option<RedisSorted>,
    working: Option<RedisSorted>,
    client: RedisClient
}

pub struct RedisStore {
    inner: Arc<RwLock<RedisStoreInner>>
}

impl RedisStore {
    pub async fn new(name: String, client: RedisClient) -> RedisStore {
        let mut store = RedisStore {
            inner: Arc::new(RwLock::new(
                RedisStoreInner {
                    name,
                    queue_set: HashMap::new(),
                    scheduled: None,
                    retries: None,
                    dead: None,
                    working: None,
                    client
                }))
        };

        store.init_queue().await;
        store
    }

    pub async fn get_scheduled(&self) -> Option<RedisSorted> {
        self.inner.read().await.scheduled.clone()
    }

    pub async fn get_retires(&self) -> Option<RedisSorted> {
        self.inner.read().await.retries.clone()
    }

    pub async fn get_dead(&self) -> Option<RedisSorted> {
        self.inner.read().await.dead.clone()
    }

    pub async fn get_working(&self) -> Option<RedisSorted> {
        self.inner.read().await.working.clone()
    }

    async fn init_queue(&mut self) {
        let store = &mut *self.inner.write().await;
        store.scheduled = Some(RedisSorted {
            name: "scheduled".into(),
            store: Arc::downgrade(&self.inner)
        });

        store.retries = Some(RedisSorted {
            name: "retries".into(),
            store: Arc::downgrade(&self.inner)
        });

        store.dead = Some(RedisSorted {
            name: "dead".into(),
            store: Arc::downgrade(&self.inner)
        });

        store.working = Some(RedisSorted {
            name: "working".into(),
            store: Arc::downgrade(&self.inner)
        });
    }
}

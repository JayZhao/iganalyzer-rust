use std::collections::HashMap;
use std::sync::{Arc, Weak};

use bytes::Bytes;
use chrono::naive::NaiveDateTime;
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use tokio::sync::RwLock;

use crate::client::job::Job;
use crate::redis_client::RedisClient;
use crate::types::TaskError;
use crate::util;
use crate::working::Reservation;
use crate::Result;

#[derive(Debug)]
pub struct RedisQueue {
    name: String,
    done: bool,
    store: Weak<RwLock<RedisStoreInner>>,
}

impl RedisQueue {
    fn new(name: String, store: Weak<RwLock<RedisStoreInner>>) -> RedisQueue {
        RedisQueue {
            name,
            done: false,
            store,
        }
    }

    pub fn close(&mut self) {
        self.done = true;
    }

    pub fn name(&self) -> String {
        self.name.to_string()
    }

    pub async fn size(&self) -> Result<u64> {
        if let Some(store) = self.store.upgrade() {
            store
                .read()
                .await
                .client
                .execute::<u32, String>("LLEN", Some(&vec![self.name()]))
                .await?;
        }

        Ok(0)
    }

    pub async fn page<F>(&self, start: i64, count: i64) -> Result<Option<i64>> {
        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name(), start.to_string(), (start + count).to_string()];
            let elems = store
                .read()
                .await
                .client
                .execute::<Vec<Vec<u8>>, String>("LRANGE", Some(&args))
                .await?;

            for _elem in elems.iter() {}
        }

        Ok(None)
    }

    pub async fn clear(&self) -> Result<()> {
        if let Some(store) = self.store.upgrade() {
            let args1 = vec![self.name()];
            let args2 = vec!["queues".into(), self.name()];
            store
                .read()
                .await
                .client
                .execute::<u32, String>("UNLINK", Some(&args1))
                .await?;
            store
                .read()
                .await
                .client
                .execute::<u32, String>("SREM", Some(&args2))
                .await?;
            store.write().await.queue_set.remove(&self.name);
        }

        Ok(())
    }

    pub async fn add(&mut self, mut job: Job) -> Result<()> {
        let enqueued_at = util::utc_now();
        job.set_enqueued_at(enqueued_at);
        Ok(())
    }

    pub async fn push(&mut self, payload: &[u8]) -> Result<()> {
        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name.as_bytes(), payload];

            store
                .read()
                .await
                .client
                .execute::<u32, &[u8]>("LPUSH", Some(&args))
                .await?;
        }

        Ok(())
    }

    pub async fn pop(&mut self) -> Result<Option<Vec<u8>>> {
        if self.done {
            return Err(Box::new(TaskError::QueueEmpty(self.name.clone())));
        }

        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name()];
            return Ok(Some(
                store
                    .read()
                    .await
                    .client
                    .execute::<Vec<u8>, String>("RPOP", Some(&args))
                    .await?,
            ));
        }

        Ok(None)
    }

    pub async fn bpop(&mut self) -> Result<Option<Vec<u8>>> {
        if self.done {
            return Err(Box::new(TaskError::QueueEmpty(self.name.clone())));
        }

        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name(), "2".into()];
            return Ok(Some(
                store
                    .read()
                    .await
                    .client
                    .execute::<Vec<u8>, String>("BRPOP", Some(&args))
                    .await?,
            ));
        }

        Ok(None)
    }

    pub async fn delete(&mut self, vals: &[u8]) -> Result<()> {
        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name.as_bytes(), vals];
            return Ok(store
                .read()
                .await
                .client
                .execute::<(), &[u8]>("LREM", Some(&args))
                .await?);
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RedisSorted {
    name: String,
    store: Weak<RwLock<RedisStoreInner>>,
}

impl RedisSorted {
    fn name(&self) -> String {
        self.name.clone()
    }

    pub async fn size(&self) -> Result<Option<usize>> {
        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name.clone()];

            return Ok(store
                .read()
                .await
                .client
                .execute::<Option<usize>, String>("ZCARD", Some(&args))
                .await?);
        }

        Ok(None)
    }

    pub async fn clear(&self) -> Result<()> {
        if let Some(store) = self.store.upgrade() {
            let args = vec![self.name.clone()];

            store
                .read()
                .await
                .client
                .execute::<(), String>("UNLINK", Some(&args))
                .await?;
        }

        Ok(())
    }

    pub async fn add(&self, job: Job) -> Result<()> {
        let at = job.at.clone();
        if at.is_none() || at == Some("".into()) {
            return Err(Box::new(TaskError::JobErr("at is empty".into())));
        }
        self.add_elem(&at.clone().unwrap(), job.encode()?).await?;
        Ok(())
    }

    pub async fn add_elem(&self, timestamp: &str, payload: Bytes) -> Result<()> {
        let at = util::parse_time(&timestamp);

        let time_f = (at.timestamp_nanos() as f64 / 1000000000.0).to_string();

        let name = self.name().clone();

        let args = vec![name.as_bytes(), time_f.as_bytes(), payload.as_ref()];

        if let Some(store) = self.store.upgrade() {
            store
                .read()
                .await
                .client
                .execute::<usize, &[u8]>("ZADD", Some(&args))
                .await?;
        }

        Ok(())
    }

    // timestmap|jid
    pub fn decompose(key: &str) -> Result<(f64, String)> {
        let slice = key.split("|").collect::<Vec<&str>>();

        if slice.len() != 2 {
            return Err(Box::new(TaskError::KeyInvalid(key.into())));
        }
        let timestamp = slice[0];
        let time = util::parse_time(timestamp);

        Ok((
            time.timestamp_nanos() as f64 / 1000000000.0,
            slice[1].to_string(),
        ))
    }

    pub async fn get_score(&self, score: f64) -> Result<Option<Vec<Vec<u8>>>> {
        if let Some(store) = self.store.upgrade() {
            let score = score.to_string();
            let args = vec![self.name.as_bytes(), score.as_bytes(), score.as_bytes()];

            return Ok(store
                .read()
                .await
                .client
                .execute::<Option<Vec<Vec<u8>>>, &[u8]>("ZRangeByScore", Some(&args))
                .await?);
        }

        Ok(None)
    }

    pub async fn get(&self, key: &str) -> Result<Option<SetEntry>> {
        if let Ok((timestamp, jid)) = RedisSorted::decompose(key) {
            let elems = self.get_score(timestamp).await?;

            if let Some(elems) = elems {
                if elems.len() == 1 {
                    return Ok(Some(SetEntry::new(
                        Bytes::copy_from_slice(elems[0].as_ref()),
                        timestamp,
                    )));
                }

                if elems.len() > 1 {
                    for elem in elems {
                        let job = Job::decode(&elem)?;

                        if job.jid == jid {
                            return Ok(Some(SetEntry::new(
                                Bytes::copy_from_slice(elem.as_ref()),
                                timestamp,
                            )));
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    pub async fn find<F>(&self, m: &str, f: F) -> Result<()>
    where
        F: Fn((i32, SetEntry)),
    {
        if let Some(store) = self.store.upgrade() {
            let args = vec![
                self.name(),
                "0".into(),
                "MATCH".into(),
                m.into(),
                "COUNT".into(),
                "100".into(),
            ];
            let elems = store
                .read()
                .await
                .client
                .execute::<Option<Vec<Vec<String>>>, String>("ZSCAN", Some(&args))
                .await?;

            if let Some(mut elems) = elems {
                if elems.len() == 1 {
                    let elem = elems.pop().unwrap();
                    let mut iter = elem.iter();
                    let size = elem.len() as i32;
                    let mut counter = 0;
                    loop {
                        let job = if let Some(job_item) = iter.next() {
                            Some(job_item)
                        } else {
                            None
                        };

                        let score = if let Some(score_item) = iter.next() {
                            Some(score_item.parse::<f64>()?)
                        } else {
                            None
                        };

                        if let Some(job) = job {
                            if let Some(score) = score {
                                f((
                                    counter,
                                    SetEntry::new(Bytes::copy_from_slice(job.as_bytes()), score),
                                ));
                            }
                        }

                        counter = counter + 1;
                        if counter >= size {
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn page(&self, start: i32, count: i32) -> Result<Option<(i32, Vec<SetEntry>)>> {
        let mut entries = vec![];

        if let Some(store) = self.store.upgrade() {
            let args = vec![
                self.name(),
                start.to_string(),
                (start + count).to_string(),
                "WITHSCORES".into(),
            ];
            let elems = store
                .read()
                .await
                .client
                .execute::<Vec<String>, String>("ZRANGE", Some(&args))
                .await?;

            let mut iter = elems.iter();
            let size = elems.len() as i32;
            let mut counter = 0;
            loop {
                let job = if let Some(job_item) = iter.next() {
                    Some(job_item)
                } else {
                    None
                };

                let score = if let Some(score_item) = iter.next() {
                    Some(score_item.parse::<f64>()?)
                } else {
                    None
                };

                if let Some(job) = job {
                    if let Some(score) = score {
                        entries.push(SetEntry::new(Bytes::copy_from_slice(job.as_bytes()), score));
                    } else {
                        break;
                    }
                } else {
                    break;
                }

                counter = counter + 1;
                if counter >= size {
                    break;
                }
            }

            return Ok(Some((counter, entries)));
        }

        Ok(None)
    }

    pub async fn each(&self) -> Result<Option<(i32, Vec<SetEntry>)>> {
        let count = 50;
        let mut current = 0;
        let mut res = vec![];

        loop {
            if let Some((size, entries)) = self.page(current, count).await? {
                for entry in entries {
                    res.push(entry);
                }

                if size < count {
                    current += size;
                    break;
                }

                current += count;
            } else {
                break;
            }
        }

        Ok(Some((current, res)))
    }

    pub async fn rem(&self, times_f: f64, jid: String) -> Result<bool> {
        if let Some(store) = self.store.upgrade() {
            let elems = self.get_score(times_f).await?;

            if let Some(elems) = elems {
                if elems.len() == 1 {
                    let elem = Bytes::copy_from_slice(elems[0].as_ref());
                    let args = vec![self.name.as_bytes(), &elem];
                    let result = store
                        .read()
                        .await
                        .client
                        .execute::<i32, &[u8]>("ZREM", Some(&args))
                        .await?;
                    if result == 1 {
                        return Ok(true);
                    }
                }

                if elems.len() > 1 {
                    for elem in elems {
                        let job = Job::decode(&elem)?;

                        if job.jid == jid {
                            let elem = Bytes::copy_from_slice(elem.as_ref());
                            let args = vec![self.name.as_bytes(), &elem];
                            let result = store
                                .read()
                                .await
                                .client
                                .execute::<i32, &[u8]>("ZREM", Some(&args))
                                .await?;
                            if result == 1 {
                                return Ok(true);
                            }
                        }
                    }
                }
            }
        }

        Ok(false)
    }

    pub async fn remove(&self, key: &str) -> Result<bool> {
        if let Ok((timestamp, jid)) = RedisSorted::decompose(key) {
            if let Some(_store) = self.store.upgrade() {
                return self.rem(timestamp, jid).await;
            }
        }

        Ok(false)
    }

    pub async fn remove_elem(&self, timestamp: &str, jid: &str) -> Result<bool> {
        if let Some(_store) = self.store.upgrade() {
            let time = util::parse_time(timestamp);
            let time_f = time.timestamp_nanos() as f64 / 1000000000.0;
            return self.rem(time_f, jid.into()).await;
        }

        Ok(false)
    }

    pub async fn remove_before<F>(
        &self,
        timestamp: &str,
        max_count: i64,
        mut f: F,
    ) -> Result<Option<i64>>
    where
        F: FnMut(String, f64),
    {
        if let Some(store) = self.store.upgrade() {
            let time = util::parse_time(timestamp);
            let time_f = time.timestamp_nanos() as f64 / 1000000000.0;
            let args = vec![
                self.name(),
                "-INF".into(),
                time_f.to_string(),
                "WITHSCORES".into(),
                "LIMIT".into(),
                "0".into(),
                max_count.to_string(),
            ];
            let elems = store
                .read()
                .await
                .client
                .execute::<Vec<String>, String>("ZRANGEBYSCORE", Some(&args))
                .await?;

            let mut iter = elems.iter();
            let size = elems.len() as i32;
            let mut counter = 0;
            loop {
                let item = if let Some(item) = iter.next() {
                    Some(item)
                } else {
                    None
                };

                let score = if let Some(score_item) = iter.next() {
                    Some(score_item.parse::<f64>()?)
                } else {
                    None
                };

                if let Some(item) = item {
                    if let Some(score) = score {
                        let args = vec![self.name(), item.into()];
                        store
                            .read()
                            .await
                            .client
                            .execute::<i32, String>("ZREM", Some(&args))
                            .await?;
                        f(item.into(), score);
                    } else {
                        break;
                    }
                } else {
                    break;
                }

                counter += 1;
                if counter >= size {
                    break;
                }
            }

            return Ok(Some(counter as i64));
        }

        Ok(None)
    }

    pub async fn move_to<F>(
        &self,
        sorted_set: &RedisSorted,
        entry: &SetEntry,
        new_time: DateTime<Utc>,
    ) -> Result<bool> {
        if let Some(store) = self.store.upgrade() {
            let job = entry.job()?;
            let args = vec![self.name(), String::from_utf8_lossy(entry.value()).into()];
            store
                .read()
                .await
                .client
                .execute::<i32, String>("ZREM", Some(&args))
                .await?;
            sorted_set
                .add_elem(
                    &new_time.to_rfc3339_opts(SecondsFormat::Nanos, true),
                    Bytes::copy_from_slice(job.jid.as_bytes()),
                )
                .await?;

            return Ok(true);
        }

        Ok(false)
    }
}

#[derive(Debug, Clone)]
pub struct SetEntry {
    value: Bytes,
    score: f64,
}

impl SetEntry {
    pub fn new(value: Bytes, score: f64) -> SetEntry {
        SetEntry { value, score }
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub fn key(&self) -> Result<String> {
        let secs = self.score as i64;
        let nsecs = ((self.score - secs as f64) * 1000000000.0) as u32;
        let datetime = NaiveDateTime::from_timestamp(secs, nsecs);
        let utc = DateTime::<Utc>::from_utc(datetime, Utc);
        let st = utc.to_rfc3339_opts(SecondsFormat::Nanos, true);
        let job = self.job()?;

        Ok(format!("{}|{}", st, job.jid))
    }

    pub fn job(&self) -> Result<Job> {
        Ok(Job::decode(&self.value)?)
    }

    pub fn reservation(&self) -> Result<Reservation> {
        Ok(Reservation::decode(&self.value).unwrap())
    }
}

#[derive(Debug)]
struct RedisStoreInner {
    name: String,
    queue_set: HashMap<String, RedisQueue>,
    scheduled: Option<RedisSorted>,
    retries: Option<RedisSorted>,
    dead: Option<RedisSorted>,
    working: Option<RedisSorted>,
    client: RedisClient,
}

#[derive(Debug, Clone)]
pub struct RedisStore {
    inner: Arc<RwLock<RedisStoreInner>>,
}

impl RedisStore {
    pub async fn new(name: String, client: RedisClient) -> RedisStore {
        let mut store = RedisStore {
            inner: Arc::new(RwLock::new(RedisStoreInner {
                name,
                queue_set: HashMap::new(),
                scheduled: None,
                retries: None,
                dead: None,
                working: None,
                client,
            })),
        };

        store.init_queue().await;
        store
    }

    pub async fn success(&self) -> Result<()> {
        let client = &self.inner.read().await.client;
        let args = vec!["processed"];

        client.execute::<i32, &str>("INCR", Some(&args)).await?;

        Ok(())
    }

    pub async fn total_success(&self) -> Result<i32> {
        let client = &self.inner.read().await.client;

        let args = vec!["processed", "0"];

        let r = client.execute::<i32, &str>("INCRBY", Some(&args)).await?;

        Ok(r)
    }

    pub async fn fail(&self) -> Result<()> {
        let client = &self.inner.read().await.client;
        let args = vec!["failures"];

        client.execute::<i32, &str>("INCR", Some(&args)).await?;

        Ok(())
    }

    pub async fn total_failures(&self) -> Result<i32> {
        let client = &self.inner.read().await.client;
        let args = vec!["failures", "0"];

        let r = client.execute::<i32, &str>("INCRBY", Some(&args)).await?;

        Ok(r)
    }

    pub async fn retry_later(&mut self, mut job: Job) -> Result<()> {
        if let Some(when) = job.next_try() {
            if let Some(failure) = &mut job.failure {
                let time = util::format_time(when);
                failure.next_at = time.clone();
                let payload = Job::encode(&job)?;
                if let Some(retries) = self.get_retries().await {
                    retries.add_elem(&time, payload).await?;
                }
            }
        }
        Ok(())
    }

    pub async fn send_to_morgue(&self, job: Job) -> Result<()> {
        let time = util::format_time(Utc::now() + Duration::hours(24 * 180));

        let payload = Job::encode(&job)?;
        if let Some(dead) = self.get_dead().await {
            dead.add_elem(&time, payload).await?;
        }

        Ok(())
    }

    pub async fn get_scheduled(&self) -> Option<RedisSorted> {
        self.inner.read().await.scheduled.clone()
    }

    pub async fn get_retries(&self) -> Option<RedisSorted> {
        self.inner.read().await.retries.clone()
    }

    pub async fn get_dead(&self) -> Option<RedisSorted> {
        self.inner.read().await.dead.clone()
    }

    pub async fn get_working(&self) -> Option<RedisSorted> {
        self.inner.read().await.working.clone()
    }

    pub async fn fetch_queue_then<F, U>(&self, name: &str, f: F)
    where
        F: FnOnce(RedisQueue) -> U + Send + 'static,
        U: std::future::Future<Output = RedisQueue> + Send + 'static,
    {
        let mut inner = self.inner.write().await;
        if let Some(queue) = inner.queue_set.remove(name) {
            let queue = f(queue).await;
            inner.queue_set.insert(name.into(), queue);
        } else {
            drop(inner);
            let queue = RedisQueue::new(name.into(), Arc::downgrade(&self.inner));
            let queue = f(queue).await;
            let mut inner = self.inner.write().await;
            inner.queue_set.insert(name.into(), queue);
        }
    }

    pub async fn flush(&self) -> Result<bool> {
        let client = &self.inner.read().await.client;
        let r = client.execute::<i32, &[u8]>("FLUSHALL", None).await?;

        if r == 1 {
            return Ok(true);
        }

        Ok(false)
    }

    pub async fn brpop(&self, queues: Vec<&str>) -> Result<Option<Vec<String>>> {
        let client = &self.inner.read().await.client;
        let args = [queues, vec!["2"]].concat();

        let r = client
            .execute::<Option<Vec<String>>, &str>("BRPOP", Some(&args))
            .await?;

        Ok(r)
    }

    pub async fn init_queue(&mut self) {
        let store = &mut *self.inner.write().await;
        store.scheduled = Some(RedisSorted {
            name: "scheduled".into(),
            store: Arc::downgrade(&self.inner),
        });

        store.retries = Some(RedisSorted {
            name: "retries".into(),
            store: Arc::downgrade(&self.inner),
        });

        store.dead = Some(RedisSorted {
            name: "dead".into(),
            store: Arc::downgrade(&self.inner),
        });

        store.working = Some(RedisSorted {
            name: "working".into(),
            store: Arc::downgrade(&self.inner),
        });
    }
}

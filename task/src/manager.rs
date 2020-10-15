use log::*;
use chrono::{Utc, DateTime};
use bytes::Buf;
use std::collections::HashMap;
use crate::store::RedisStore;
use crate::store::RedisSorted;
use crate::client::job::Job;
use crate::working::Reservation;

pub struct Manager<'a> {
    store: RedisStore,
    working_map: HashMap<String, Reservation<'a>>
}


impl<'a> Manager<'a> {
    pub fn new(store: RedisStore) -> Manager<'a> {
        Manager {
            store,
            working_map: HashMap::new()
        }
    }

    pub async fn load_working_set(&self) {
        if let Some(working) = self.store.get_working().await {
            working.each(|(score, entry)| {
                let res = entry.reservation();
            });
        }
    }
    
    async fn push(&self, mut job: Job) {
        if job.jid == "" || job.jid.len() < 8 {
            error!("All jobs must have a reasonable jid parameter");
        }

        if job.job_type == "" {
            error!("All jobs must have a jobtype parameter");
        }

        if job.args.is_none() {
            error!("All jobs must have an args parameter");
        }

        if let Some(reserve_for) = job.reserve_for {
            if reserve_for < 86400 {
                error!("Jobs cannot be reserved for more than one day");
            }
        }

        if job.created_at.is_none() {
            job.created_at = Some(Utc::now().to_rfc3339());
        }

        if job.queue == "" {
            job.queue = "default".into();
        }

        if let Some(at) = job.at.clone() {
            if at != "" {
                let time_at = DateTime::parse_from_rfc3339(&at);

                match time_at {
                    Ok(time_at) => {
                        if time_at < Utc::now() {
                            if let Some(scheduled) = self.store.get_scheduled().await {
                                if let Ok(job) = Job::encode(&job) {
                                    if let Err(e) = scheduled.add_elem(&at, job).await {
                                        error!("Job cannot be pushed into the schedued queue, {:?}", e);
                                    }
                                }
                            }

                        } else {
                            self.enqueue(job).await;
                        }
                    },

                    Err(_e) => {
                        error!("Invalid timestamp {:?}", at);
                    }
                }
            }
        }
    }

    pub async fn purge(&self, when: &str) {
        if let Some(dead) = self.store.get_dead().await {
            match dead.remove_before(when, 100 as i64, |job, score| {
                debug!("Remove job {:?} with score {:?}", job, score);
            }).await {
                Ok(count) => {
                    info!("Remove {:?} jobs", count);
                },
                Err(e) => {
                    error!("Err when remove jobs, {:?}", e);
                }
            }
        }
    }

    pub async fn enqueue(&self, mut job: Job) {
        self.store.fetch_queue_then(&job.queue.clone(), |mut queue| {
            async move {
                job.enqueued_at = Some(Utc::now().to_rfc3339());

                if let Ok(job) = Job::encode(&job) {
                    if let Err(e) = queue.push(job.as_ref()).await {
                        error!("Job cannot be pushed into the queue, {:?}", e);
                    }
                }

                queue
            }
        }).await;
    }

    pub async fn schedule(&self, when: &str) -> i64 {
        
        if let Some(scheduled) = self.store.get_scheduled().await {
            return self.shift(when, scheduled).await;
        }

        0
    }

    pub async fn retry(&self, when: &str) -> i64 {
        if let Some(retries) = self.store.get_retries().await {
            return self.shift(when, retries).await;
        }

        0

    }

    async fn shift(&self, when: &str, queue: RedisSorted) -> i64 {
        let mut total: i64 = 0;
        let mut jobs = vec![];
        loop {
            match queue.remove_before(when, 100 as i64, |job, _score | {
                if let Ok(job) = Job::decode(&job.as_ref()) {
                    jobs.push(job);
                }
            }).await {
                Ok(count) => {
                    if let Some(count) = count {
                        total += count;

                        if count != 100 {
                            break;
                        }
                    } else {
                        break;
                    }
                },
                Err(e) => {
                    error!("Error pushing job {:?}", e);
                }
            }
        }

        for job in jobs {
            self.enqueue(job).await;
        }

        total
    }
}

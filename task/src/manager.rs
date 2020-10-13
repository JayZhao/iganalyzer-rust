use log::*;
use chrono::{Utc, DateTime};
use bytes::Buf;
use crate::store::RedisStore;
use crate::client::job::Job;

struct Manager {
    store: RedisStore,
}


impl Manager {
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
                                    scheduled.add_elem(&at, job.into()).await;
                                }
                            }
                        } else {
                            self.store.fetch_queue_then(&job.queue.clone(), |mut queue| {
                                job.enqueued_at = Some(Utc::now().to_rfc3339());

                                if let Ok(job) = Job::encode(&job) {
                                    queue.push(job.as_ref());
                                }

                                Ok(())
                            });
                        }
                    },

                    Err(e) => {
                        error!("Invalid timestamp {:?}", at);
                    }
                }
            }
        }
    }

    async fn enqueue(&self, job: Job) {
        
    }
}

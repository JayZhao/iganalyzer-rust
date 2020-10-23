use crate::client::job::Failure;
use crate::client::job::Job;
use crate::store::RedisSorted;
use crate::store::RedisStore;
use crate::types::TaskError;
use crate::util;
use crate::working::Reservation;
use crate::Result;
use chrono::{DateTime, Utc};
use log::*;
use std::collections::HashMap;

#[derive(Debug)]
pub struct Manager {
    store: RedisStore,
    working_map: HashMap<String, Reservation>,
}

#[derive(Debug, PartialEq)]
pub enum FailErrType {
    ReservationExpired,
}

#[derive(Debug)]
pub struct FailPayload {
    jid: String,
    err_msg: String,
    err_type: FailErrType,
    backtrace: Vec<String>,
}

impl Manager {
    pub fn new(store: RedisStore) -> Manager {
        Manager {
            store,
            working_map: HashMap::new(),
        }
    }

    pub async fn load_working_set(&mut self) -> Result<()> {
        if let Some(working) = self.store.get_working().await {
            if let Ok(Some((counter, entries))) = working.each().await {
                info!("Loading {} working jobs", counter);
                for entry in entries {
                    match entry.reservation() {
                        Ok(r) => {
                            self.working_map.insert(r.job.jid.to_string(), r);
                        }
                        Err(e) => {
                            error!("Failed loading {:?} job, {:?}", entry, e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn reap_expired_jobs(&mut self, when: &str) -> Result<i64> {
        let mut total = 0;
        let mut ress = vec![];

        loop {
            if let Some(working) = self.store.get_working().await {
                match working
                    .remove_before(when, 10, |item, _score| {
                        match Reservation::decode(item.as_bytes()) {
                            Ok(res) => {
                                ress.push(res);
                            }
                            Err(e) => {
                                error!("invalid reservation {:?} {:?}", item, e);
                            }
                        }
                    })
                    .await
                {
                    Ok(Some(counter)) => {
                        if counter < 10 {
                            break;
                        }
                    }
                    Ok(None) => debug!("no expired jobs"),
                    Err(e) => {
                        error!("reap expired jobs failed, {:?}", e);
                    }
                }
            }
        }

        for res in ress {
            let job = &res.job;
            let jid = &job.jid;

            if let Some(local_res) = self.working_map.get_mut(jid) {
                if let Some(extension) = local_res.extension {
                    if util::parse_time(when) < extension {
                        local_res.texpiry = Some(extension);
                        local_res.expiry = util::format_time(extension);

                        info!(
                            "Auto-extending reservation time for {:?} to {:?}",
                            jid, local_res.expiry
                        );
                        if let Some(working) = self.store.get_working().await {
                            let r = Reservation::encode(local_res).unwrap();
                            if let Err(e) = working.add_elem(&local_res.expiry, r).await {
                                error!("Push reservation job failed {:?}", e);
                            }

                            continue;
                        }
                    }
                }

                let job = res.job;

                self.process_failure(
                    &job.jid,
                    FailPayload {
                        jid: job.jid.clone(),
                        err_msg: "reservation expired".into(),
                        err_type: FailErrType::ReservationExpired,
                        backtrace: vec![],
                    },
                )
                .await?;

                total += 1;
            }
        }

        Ok(total)
    }

    pub async fn clear_reservation(&mut self, jid: &str) -> Result<Reservation> {
        let res = self.working_map.remove(jid);

        if let None = res {
            error!("Job not found, {:?}", res);

            return Err(Box::new(TaskError::JobErr("Job not found".into())));
        }

        Ok(res.unwrap())
    }

    pub async fn process_failure(&mut self, jid: &str, fail_payload: FailPayload) -> Result<()> {
        let res = self.clear_reservation(jid).await?;

        if fail_payload.err_type != FailErrType::ReservationExpired {
            if let Some(working) = self.store.get_working().await {
                working.remove_elem(&res.expiry, jid).await?;
            }
        }

        self.store.fail().await?;

        let mut job = res.job;

        if let Some(mut failure) = job.failure.as_mut() {
            failure.retry_count += 1;
            failure.err_msg = fail_payload.err_msg;
            failure.err_type = format!("{:?}", fail_payload.err_type);
            failure.backtrace = fail_payload.backtrace;
        } else {
            job.failure = Some(Failure {
                retry_count: 0,
                failed_at: util::utc_now(),
                next_at: "".into(),
                err_msg: fail_payload.err_msg,
                err_type: format!("{:?}", fail_payload.err_type),
                backtrace: fail_payload.backtrace,
            });
        }

        if let Some(count) = &job.retry {
            if let Some(failure) = &job.failure {
                if *count > 0 && failure.retry_count < *count {
                    self.store.retry_later(job).await?;
                } else {
                    self.store.send_to_morgue(job).await?;
                }
            }
        }

        Ok(())
    }

    pub async fn push(&self, mut job: Job) -> Result<()> {
        if job.jid == "" || job.jid.len() < 8 {
            return Err(Box::new(TaskError::JobErr(
                "All jobs must have a reasonable jid parameter".into(),
            )));
        }

        if job.job_type == "" {
            return Err(Box::new(TaskError::JobErr(
                "All jobs must have a jobtype parameter".into(),
            )));
        }

        if job.args.is_none() {
            return Err(Box::new(TaskError::JobErr(
                "All jobs must have an args parameter".into(),
            )));
        }

        if let Some(reserve_for) = job.reserve_for {
            if reserve_for < 86400 {
                return Err(Box::new(TaskError::JobErr(
                    "Jobs cannot be reserved for more than one day".into(),
                )));
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
                                        return Err(Box::new(TaskError::JobErr(format!(
                                            "Job cannot be pushed into the schedued queue: {:?}",
                                            e
                                        ))));
                                    }
                                }
                            }
                        }
                    }
                    Err(_e) => {
                        return Err(Box::new(TaskError::JobErr("Invalid timestamp".into())));
                    }
                }
            }
        } else {
            self.enqueue(job).await;
        }

        Ok(())
    }

    pub async fn purge(&self, when: &str) {
        if let Some(dead) = self.store.get_dead().await {
            match dead
                .remove_before(when, 100 as i64, |job, score| {
                    debug!("Remove job {:?} with score {:?}", job, score);
                })
                .await
            {
                Ok(count) => {
                    info!("Remove {:?} jobs", count);
                }
                Err(e) => {
                    error!("Err when remove jobs, {:?}", e);
                }
            }
        }
    }

    pub async fn enqueue(&self, mut job: Job) {
        self.store
            .fetch_queue_then(&job.queue.clone(), |mut queue| async move {
                job.enqueued_at = Some(Utc::now().to_rfc3339());

                if let Ok(job) = Job::encode(&job) {
                    info!("job {:?}", job);
                    if let Err(e) = queue.push(job.as_ref()).await {
                        error!("Job cannot be pushed into the queue, {:?}", e);
                    }
                }

                queue
            })
            .await;
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

    pub async fn fetch(&self, queues: Vec<&str>) -> Result<Option<Job>> {
        let res = self.store.brpop(queues).await?;
        match res {
            Some(payload) => {
                let queue = &payload[0];
                info!("Fetching the {:?} queue's job", queue);
                let job = Job::decode(payload[1].as_bytes())?;

                Ok(Some(job))
            }
            None => Ok(None),
        }
    }

    async fn shift(&self, when: &str, queue: RedisSorted) -> i64 {
        let mut total: i64 = 0;
        let mut jobs = vec![];
        loop {
            match queue
                .remove_before(when, 100 as i64, |job, _score| {
                    if let Ok(job) = Job::decode(&job.as_ref()) {
                        jobs.push(job);
                    }
                })
                .await
            {
                Ok(count) => {
                    if let Some(count) = count {
                        total += count;

                        if count != 100 {
                            break;
                        }
                    } else {
                        break;
                    }
                }
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

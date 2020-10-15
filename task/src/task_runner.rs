use std::sync::atomic::{AtomicI64, Ordering};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use async_trait::async_trait;
use chrono::{Utc, DateTime};
use log::*;
use std::time::Duration;
use tokio::sync::broadcast;
use std::fmt;

use crate::shutdown::Shutdown;
use crate::Error;
use crate::Result;
use crate::store::RedisSorted;

#[async_trait]
pub trait Taskable {
    fn name(&self) -> &str;
    async fn execute(&mut self) -> Result<()>;
    async fn stats(&self) -> Result<HashMap<String, String>>;
}

#[derive(Debug)]
struct Task<T> where T: Taskable + Send + Unpin + Sized {
    runner: Pin<Box<T>>,
    every: AtomicI64,
    runs: AtomicI64,
    walltime_ns: AtomicI64
}

impl<T> Task<T> where T: Taskable + Send + Unpin + Sized {
    fn new(runner: Pin<Box<T>>, every: AtomicI64) -> Task<T> {
        Task {
            runner, every, runs: AtomicI64::new(0),
            walltime_ns: AtomicI64::new(0)
        }
    }
}

#[derive(Debug)]
pub struct TaskRunner<T>
    where T: Taskable + Send + Unpin + Sized
{
    tasks: Vec<Task<T>>,
    walltime_ns: AtomicI64,
    cycles: AtomicI64,
    executions: AtomicI64,
    shutdown: Shutdown,
}

impl<T> fmt::Display for TaskRunner<T>
    where T: Taskable + Send + Unpin + Sized
{
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "Runner: walltime_ns: {:?}, cycles: {:?}, executions: {:?}", self.walltime_ns, self.cycles, self.executions)
    }
}


impl<T> TaskRunner<T>
    where T: Taskable + Send + Unpin + Sized + 'static
{
    pub fn new(shutdown: Shutdown) -> TaskRunner<T> {
        TaskRunner {
            tasks: vec![],
            walltime_ns: AtomicI64::new(0),
            cycles: AtomicI64::new(0),
            executions: AtomicI64::new(0),
            shutdown
        }
    }

    pub async fn add_task(&mut self, sec: i64, thing: T) {
        let task = Task::new(Pin::new(Box::new(thing)), AtomicI64::new(sec));
        self.tasks.push(task);
    }

    pub async fn run(mut runner: TaskRunner<T>)
    {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = runner.shutdown.recv() => {
                        info!("Task Runner shutdown");
                        return;
                    }
                    _ = tokio::time::delay_for(Duration::from_millis(1000)) => {
                        debug!("Task Runner {}", runner);
                        runner.cycle().await;
                    }
                };
            }
        });
    }

    pub async fn cycle(&mut self) {
        let mut count: i64 = 0;
        let start = Utc::now();
        let sec = start.timestamp();

        for task in &mut self.tasks {
            let every = task.every.load(Ordering::Relaxed);
            if sec % every != 0 {
                continue;
            }

            info!("{:?} {:?}", sec, every);

            let tstart = Utc::now();

            if let Err(e) = task.runner.execute().await {
                error!("Task execute failed. {:?}", e);
            }

            let tend = Utc::now();

            let td = (tend - tstart).num_nanoseconds().unwrap();

            task.runs.fetch_add(1, Ordering::SeqCst);
            task.walltime_ns.fetch_add(td, Ordering::SeqCst);
            count = count + 1;

        }

        let end = Utc::now();
        let d = (end - start).num_nanoseconds().unwrap();
        self.cycles.fetch_add(1, Ordering::SeqCst);
        self.executions.fetch_add(count, Ordering::SeqCst);
        self.walltime_ns.fetch_add(d, Ordering::SeqCst);
        
    }
}

#[derive(Debug)]
pub struct Scanner {
    pub name: String,
    pub category: ScannerCategory,
    pub set: RedisSorted,
    pub jobs: AtomicI64,
    pub cycles: AtomicI64,
    pub walltime: AtomicI64
}

#[derive(Debug)]
pub enum ScannerCategory {
    Scheduled,
    Retry,
    Dead
}

impl Scanner {
    pub fn new(name: String, category: ScannerCategory, set: RedisSorted) -> Scanner {
        Scanner {
            name, set, category,
            jobs: AtomicI64::new(0),
            cycles: AtomicI64::new(0),
            walltime: AtomicI64::new(0)
        }
    }
    
    async fn task(&self, time: DateTime<Utc>) -> Result<i64> {

        match self.category {
            ScannerCategory::Scheduled => {
                info!("S");
            },
            ScannerCategory::Retry => {
                info!("R");
            },
            ScannerCategory::Dead => {
                info!("D");
            }
        }
        Ok(0)
    }
}

#[async_trait]
impl Taskable for Scanner {

    fn name(&self) -> &str {
        &self.name
    }

    async fn execute(&mut self) -> Result<()> {
        let start = Utc::now();

        let count = self.task(start).await?;

        if count > 0 {
            info!("{:?} processed {:?} jobs", self.name, count);
        }
        let end = Utc::now();

        let dur = (end - start).num_nanoseconds().unwrap();

        self.cycles.fetch_add(1, Ordering::SeqCst);
        self.jobs.fetch_add(count, Ordering::SeqCst);
        self.walltime.fetch_add(dur, Ordering::SeqCst);
            
        Ok(())
    }

    async fn stats(&self) -> Result<HashMap<String, String>> {
        let mut res = HashMap::new();

        res.insert("enqueued".into(), self.jobs.load(Ordering::Relaxed).to_string());
        res.insert("cycles".into(), self.cycles.load(Ordering::Relaxed).to_string());

        match self.set.size().await {
            Ok(Some(size)) => {
                res.insert("size".into(), size.to_string());
            },
            Ok(None) => {
                error!("empty queue: {:?}", self.name);
            }
            Err(e) => {
                error!("cannot fetch queue size: {:?}", e);
            }
        }
        
        res.insert("wall_time_sec".into(), (self.walltime.load(Ordering::Relaxed) as f64 / 1000_000_000.0).to_string());

        Ok(res)
    }
}
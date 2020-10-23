use crate::client::job;
use crate::connection::Connection;
use crate::manager::Manager;
use crate::server::ServerContext;
use crate::server::WorkerState;
use crate::server::Workers;
use crate::store::RedisStore;
use crate::Result;
use chrono::Utc;
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str;
use std::sync::Arc;
use tokio::sync::RwLock;

pub const COMMAND_SET: [&str; 11] = [
    "END", "PUSH", "FETCH", "ACK", "FAIL", "BEAT", "INFO", "FLUSH", "MUTATE", "BATCH", "TRACK",
];

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientBeat {
    pub current_state: Option<WorkerState>,
    pub wid: String,
}

impl ClientBeat {
    fn decode(buf: &[u8]) -> Result<ClientBeat> {
        Ok(serde_json::from_str(&String::from_utf8_lossy(buf))?)
    }
}
async fn flush(store: &RedisStore) -> Result<()> {
    store.flush().await?;

    Ok(())
}

async fn push(manager: Arc<RwLock<Manager>>, conn: &mut Connection, cmd: &str) -> Result<()> {
    debug!("pushing............");
    match job::Job::decode(cmd.as_ref()) {
        Ok(mut job) => {
            info!("{:?}", job);
            job.retry = Some(0); // default
            job.jid = job::Job::random_jid();
            job.created_at = Some(Utc::now().to_rfc3339());
            debug!("{:?}", job);
            match manager.write().await.push(job).await {
                Ok(()) => {
                    conn.send_ok().await?;
                }
                Err(e) => {
                    conn.send_err(e).await?;
                }
            }
        }
        Err(e) => {
            conn.send_err(e.into()).await?;
        }
    };

    Ok(())
}

async fn fetch(manager: Arc<RwLock<Manager>>, conn: &mut Connection, cmd: &str) -> Result<()> {
    info!("fetching..............");

    let queues = cmd.split(' ').collect::<Vec<&str>>();
    let job = manager.read().await.fetch(queues).await;

    match job {
        Ok(Some(job)) => match job::Job::encode(&job) {
            Ok(bytes) => {
                conn.send_msg(str::from_utf8(&bytes).unwrap()).await?;
            }
            Err(e) => {
                conn.send_err(e.into()).await?;
            }
        },
        Ok(None) => {
            info!("Empty queues");
        }

        Err(e) => {
            conn.send_err(e).await?;
        }
    };

    Ok(())
}

async fn ack() -> Result<()> {
    Ok(())
}

async fn fail() -> Result<()> {
    Ok(())
}

async fn heartbeat(workers: Arc<RwLock<Workers>>, conn: &mut Connection, cmd: &str) -> Result<()> {
    match ClientBeat::decode(cmd.as_ref()) {
        Ok(beat) => {
            let mut workers = workers.write().await;
            workers.heartbeats(beat).await;
            conn.send_ok().await?;
        }
        Err(e) => {
            conn.send_err(e).await?;
        }
    };

    Ok(())
}

async fn info(server_context: Arc<RwLock<ServerContext>>, conn: &mut Connection) -> Result<()> {
    let mut state: HashMap<String, String> = HashMap::new();
    let context = server_context.read().await;

    state.insert("connection".into(), format!("{}", context.connections));
    state.insert("server_utc_time".into(), format!("{}", Utc::now()));
    state.insert("uptime".into(), format!("{}", context.uptime()));
    state.insert("commands".into(), format!("{}", context.cmds));
    let msg = serde_json::to_string(&state)?;

    conn.send_msg(&msg).await?;
    Ok(())
}

async fn mutable() -> Result<()> {
    Ok(())
}

async fn batch() -> Result<()> {
    unimplemented!()
}

async fn trace() -> Result<()> {
    unimplemented!()
}

async fn end(workers: Arc<RwLock<Workers>>, conn: &Connection) -> Result<()> {
    conn.close()?;

    let mut workers = workers.write().await;

    workers.remove_connection(conn).await;

    Ok(())
}

pub async fn execute(
    server_context: Arc<RwLock<ServerContext>>,
    manager: Arc<RwLock<Manager>>,
    workers: Arc<RwLock<Workers>>,
    store: &RedisStore,
    conn: &mut Connection,
    cmd: Vec<&str>,
) -> Result<()> {
    match cmd[0] {
        "END" => end(workers, conn).await?,
        "PUSH" => {
            push(manager, conn, cmd[1]).await?;
        }
        "FETCH" => fetch(manager, conn, cmd[1]).await?,
        "ACK" => ack().await?,
        "FAIL" => fail().await?,
        "BEAT" => heartbeat(workers, conn, cmd[1]).await?,
        "INFO" => {
            info(server_context, conn).await?;
        }
        "FLUSH" => flush(store).await?,
        "MUTATE" => mutable().await?,
        "BATCH" => batch().await?,
        "TRACE" => trace().await?,
        _ => {
            error!("Invalid command");
        }
    }

    Ok(())
}

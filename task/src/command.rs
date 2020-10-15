use crate::server::WorkerState;
use crate::connection::Connection;
use serde::{Serialize, Deserialize};
use log::*;
use std::sync::Arc;
use tokio::io;
use tokio::sync::RwLock;
use chrono::Utc;
use std::collections::HashMap;
use crate::client::job;
use crate::server::ServerContext;
use crate::server::Workers;
use crate::frame::Error;
use crate::store::RedisStore;

pub const COMMAND_SET: [&str; 11] = [
    "END", "PUSH", "FETCH", "ACK", "FAIL", "BEAT", "INFO", "FLUSH", "MUTATE", "BATCH", "TRACK",
];

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientBeat {
    pub current_state: Option<WorkerState>,
    pub wid: String
}

impl ClientBeat {
    fn decode(buf: &[u8]) -> Result<ClientBeat, serde_json::error::Error> {
        Ok(serde_json::from_str(&String::from_utf8_lossy(buf))?)
    }
}
async fn flush(store: &RedisStore) {
    store.flush().await;
}

async fn push(conn: &mut Connection, cmd: &str){
    match job::Job::decode(cmd.as_ref()) {
        Ok(mut job) => {
            job.retry = Some(0); // default
            job.jid = job::Job::random_jid();
            job.created_at = Some(Utc::now().to_rfc3339());
            debug!("{:?}", job);
            job
        },
        Err(e) => {
            error!("{:?}", e);

            return 
        }
    };
}                                   

async fn fetch() {
}

async fn ack() {
}

async fn fail() {
}

async fn heartbeat(workers: Arc<RwLock<Workers>>, conn: &mut Connection, cmd: &str) {
    match ClientBeat::decode(cmd.as_ref()) {
        Ok(beat) => {
            let mut workers = workers.write().await;
            workers.heartbeats(beat).await;
        },
        Err(e) => {
            error!("{:?}", e);
        }
    }
}

async fn info(server_context: Arc<RwLock<ServerContext>>, conn: &mut Connection) -> Result<(), serde_json::error::Error> {
    let mut state: HashMap<String, String> = HashMap::new();
    let context = server_context.read().await;
    state.insert("connection".into(), format!("{}", context.connections));
    state.insert("server_utc_time".into(), format!("{}", Utc::now()));
    state.insert("uptime".into(), format!("{}", context.uptime()));
    state.insert("commands".into(), format!("{}", context.cmds));
    let msg = serde_json::to_string(&state)?;
    info!("msg {:?}", msg);
    conn.send_msg(&msg).await;
    Ok(())
}

async fn mutable() {
}

async fn batch() {
    unimplemented!()
}

async fn trace() {
    unimplemented!()
}

async fn end(conn: &Connection) {
    conn.close();
}

pub async fn execute(server_context: Arc<RwLock<ServerContext>>, workers: Arc<RwLock<Workers>>, store: &RedisStore, conn: &mut Connection, cmd: Vec<&str>) {
    match cmd[0] {
        "END" => end(conn).await,
        "PUSH" => {
            push(conn, cmd[1]).await;
        },
        "FETCH" => fetch().await,
        "ACK" => ack().await,
        "FAIL" => fail().await,
        "BEAT" => heartbeat(workers, conn, cmd[1]).await,
        "INFO" => {
            info(server_context, conn).await;
        },
        "FLUSH" => flush(store).await,
        "MUTATE" => mutable().await,
        "BATCH" => batch().await,
        "TRACE" => trace().await,
        _ => {
            error!("Invalid command");
        }
    }
}

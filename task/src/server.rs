use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::*;
use serde::{Deserialize, Serialize};
use serde_json::error::Error;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::RwLock;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};

use crate::command::execute;
use crate::command::ClientBeat;
use crate::connection::Connection;
use crate::manager::Manager;
use crate::shutdown::Shutdown;
use crate::store::RedisStore;
use crate::task_runner::Scanner;
use crate::task_runner::ScannerCategory;
use crate::task_runner::TaskRunner;

#[derive(Debug)]
pub struct ServerContext {
    pub connections: u64,
    pub cmds: u128,
    pub started_at: DateTime<Utc>,
}

impl ServerContext {
    fn add_connection(&mut self) {
        self.connections = self.connections + 1;
    }

    fn add_cmd(&mut self) {
        self.cmds = self.cmds + 1;
    }

    fn sub_connection(&mut self) {
        self.connections = self.connections - 1;
    }

    pub fn uptime(&self) -> i64 {
        Utc::now().timestamp() - self.started_at.timestamp()
    }
}

#[derive(Debug)]
pub struct ServerOpts {
    pub bind_host: String,
    pub bind_port: u32,
}

#[derive(Debug)]
pub struct Server {
    opts: ServerOpts,
    manager: Arc<RwLock<Manager>>,
    store: RedisStore,
    listener: TcpListener,
    workers: Arc<RwLock<Workers>>,
    context: Arc<RwLock<ServerContext>>,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<String>,
    notify_reap: broadcast::Sender<String>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

#[derive(Debug)]
pub(crate) struct Handler {
    store: RedisStore,
    connection: Connection,
    manager: Arc<RwLock<Manager>>,
    server_context: Arc<RwLock<ServerContext>>,
    workers: Arc<RwLock<Workers>>,
    limit_connections: Arc<Semaphore>,
    shutdown: Shutdown,
    reap_shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum WorkerState {
    Running,
    Quit,
    Terminate,
}

// {"hostname":"127.0.0.1","wid":"test","pid":10000,"labels":["test"]}
#[derive(Serialize, Deserialize, Debug)]
pub struct ClientData {
    hostname: String,
    wid: Option<String>,
    pid: i64,
    labels: Option<Vec<String>>,
    started_at: Option<i64>,     //timestmap
    last_heartbeat: Option<i64>, // timestamp
    state: Option<WorkerState>,
}

impl ClientData {
    pub fn encode(&self) -> Result<Bytes, Error> {
        Ok(Bytes::copy_from_slice(
            serde_json::to_string(self)?.as_bytes(),
        ))
    }

    pub fn decode(buf: &[u8]) -> Result<ClientData, Error> {
        let mut data: ClientData = serde_json::from_str(&String::from_utf8_lossy(buf))?;

        data.started_at = Some(Utc::now().timestamp());
        data.last_heartbeat = Some(Utc::now().timestamp());
        data.state = Some(WorkerState::Running);

        Ok(data)
    }

    pub fn signal(&mut self, new_state: WorkerState) {
        let state = self.state.clone().unwrap();
        if state == new_state {
            return;
        }

        if state == WorkerState::Running {
            self.state = Some(new_state);
            return;
        }

        if state == WorkerState::Quit && new_state == WorkerState::Terminate {
            self.state = Some(new_state);
            return;
        }

        if state == WorkerState::Terminate {
            return;
        }
    }

    pub fn is_quiet(&self) -> bool {
        if let Some(state) = &self.state {
            return state != &WorkerState::Running;
        }

        false
    }

    pub fn is_consumer(&self) -> bool {
        self.wid.is_some()
    }
}

#[derive(Debug)]
pub struct Workers {
    heartbeats: HashMap<String, ClientData>,
}

impl Workers {
    fn new() -> Workers {
        Workers {
            heartbeats: HashMap::new(),
        }
    }

    async fn setup_heartbeats(&mut self, client_data: ClientData) {
        match client_data.wid.as_ref() {
            Some(wid) => {
                if self.heartbeats.get(wid).is_none() {
                    self.heartbeats.insert(wid.clone(), client_data);
                }
            }
            None => {
                error!("not a worker");
            }
        };
    }

    pub async fn heartbeats(&mut self, client_beat: ClientBeat) {
        if let Some(cl) = self.heartbeats.get_mut(&client_beat.wid) {
            cl.last_heartbeat = Some(Utc::now().timestamp());

            if let Some(new_state) = client_beat.current_state {
                if let Some(old_state) = &cl.state {
                    debug!("{:?} {:?}", new_state, old_state);

                    cl.last_heartbeat = Some(Utc::now().timestamp());
                    if new_state != *old_state {
                        cl.signal(new_state);
                    }
                }
            }
        }
    }

    pub async fn remove_connection(&mut self, conn: &Connection) {
        if let Some(ref wid) = conn.wid {
            info!("Deleting wid {:?}", wid);
            self.heartbeats.remove(wid);
        }
    }

    pub async fn reap_heartbeats(&mut self) -> Vec<String> {
        let mut wids = vec![];
        let time = (Utc::now() - chrono::Duration::minutes(1)).timestamp();
        for (wid, hb) in &self.heartbeats {
            if let Some(last_heartbeats) = hb.last_heartbeat {
                debug!(
                    "wid: {:?}, hearbeats: {:?},  time: {:?}",
                    wid, last_heartbeats, time
                );
                if last_heartbeats < time {
                    wids.push(wid.to_string());
                }
            }
        }

        for wid in &wids {
            self.heartbeats.remove(wid);
        }

        wids
    }
}

const MAX_CONNECTIONS: usize = 2000;

pub async fn run(opts: ServerOpts, store: RedisStore) -> crate::Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (notify_reap, _) = broadcast::channel(MAX_CONNECTIONS);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let listener = TcpListener::bind(format!("{}:{}", opts.bind_host, opts.bind_port)).await?;
    let workers = Arc::new(RwLock::new(Workers::new()));
    let context = Arc::new(RwLock::new(ServerContext {
        connections: 0,
        started_at: Utc::now(),
        cmds: 0,
    }));

    let manager = Arc::new(RwLock::new(Manager::new(store.clone())));

    let mut task_runner =
        TaskRunner::<Scanner>::new(Shutdown::new(notify_shutdown.subscribe(), None));
    let scheduled_scanner = Scanner::new(
        "scheduled".into(),
        ScannerCategory::Scheduled,
        store.get_scheduled().await.unwrap(),
        manager.clone(),
        workers.clone(),
        None,
    );
    let retries_scanner = Scanner::new(
        "retries".into(),
        ScannerCategory::Retry,
        store.get_retries().await.unwrap(),
        manager.clone(),
        workers.clone(),
        None,
    );
    let dead_scanner = Scanner::new(
        "dead".into(),
        ScannerCategory::Dead,
        store.get_dead().await.unwrap(),
        manager.clone(),
        workers.clone(),
        None,
    );

    let worker_scanner = Scanner::new(
        "workers".into(),
        ScannerCategory::ReapConn,
        store.get_dead().await.unwrap(),
        manager.clone(),
        workers.clone(),
        Some(notify_reap.clone()),
    );

    task_runner.add_task(5, scheduled_scanner).await;
    task_runner.add_task(5, retries_scanner).await;
    task_runner.add_task(60, dead_scanner).await;
    task_runner.add_task(2, worker_scanner).await;

    TaskRunner::run(task_runner).await;

    let manager = Arc::new(RwLock::new(Manager::new(store.clone())));

    let mut man = manager.write().await;
    man.load_working_set().await?;
    drop(man);

    let mut server = Server {
        opts,
        context,
        workers,
        manager,
        listener,
        store,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        notify_reap,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    let mut sig = signal(SignalKind::terminate())?;

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!("failed to accept by {:?}", err);
            }
        }
        _ = signal::ctrl_c() => {
            info!("shutting down");
        }
        _ = sig.recv() => {
            info!("shutting down");
        }
    }

    let Server {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        notify_reap,
        ..
    } = server;
    drop(notify_shutdown);
    drop(notify_reap);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

impl Server {
    async fn accept(&mut self) -> Result<TcpStream, Box<dyn std::error::Error>> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => {
                    debug!("Server stat: {:?}", self.context);
                    return Ok(socket);
                }
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }

            time::delay_for(Duration::from_secs(backoff)).await;

            backoff *= 2;
        }
    }

    async fn setup_heartbeats(&mut self, client_data: ClientData) {
        let mut ws = self.workers.write().await;
        ws.setup_heartbeats(client_data).await;
    }

    pub async fn current_state(&self) -> HashMap<String, String> {
        let mut state = HashMap::new();

        state.insert(
            "server_utc_time".into(),
            format!("{}", Utc::now().timestamp()),
        );

        state
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            self.limit_connections.acquire().await.forget();

            let socket = self.accept().await?;
            let mut conn = Connection::new(socket, Arc::clone(&self.workers));

            debug!("Recv conn {:?}", conn);
            match conn.handshake().await {
                Ok(client_data) => {
                    conn.set_wid(client_data.wid.clone());
                    self.setup_heartbeats(client_data).await;
                    let mut context = self.context.write().await;
                    context.add_connection();
                }
                Err(_e) => {
                    drop(conn);
                    continue;
                }
            };

            let wid = conn.get_wid();

            let mut handler = Handler {
                store: self.store.clone(),
                connection: conn,
                manager: Arc::clone(&self.manager),
                server_context: Arc::clone(&self.context),
                workers: Arc::clone(&self.workers),
                limit_connections: self.limit_connections.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe(), None),
                reap_shutdown: Shutdown::new(self.notify_reap.subscribe(), wid),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!("{:?}", err);
                    let mut workers = handler.workers.write().await;
                    workers.remove_connection(&handler.connection).await;
                }

                let mut context = handler.server_context.write().await;
                context.sub_connection();
            });
        }
    }
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() && !self.reap_shutdown.is_shutdown() {
            tokio::select! {
                frame = self.connection.read_frame() => {
                    match frame? {
                        Some(frame) => {
                            debug!("Frame {:?}", frame);
                            match frame.to_cmd() {
                                Ok(cmd) => {
                                    debug!("Command {:?}", cmd);
                                    let mut context = self.server_context.write().await;
                                    context.add_cmd();
                                    drop(context);
                                    if let Err(e) = execute(
                                        Arc::clone(&self.server_context),
                                        Arc::clone(&self.manager),
                                        Arc::clone(&self.workers),
                                        &self.store,
                                        &mut self.connection, cmd).await {
                                        error!("Error: {:?}", e);
                                    }
                                },
                                Err(e) => {
                                    error!("Frame Error: {:?}", e);
                                    self.connection.send_err(e.into()).await?;
                                }
                            }
                            frame
                        },
                        None => {
                            let mut workers = self.workers.write().await;
                            workers.remove_connection(&self.connection).await;
                            return Ok(());
                        }
                    }
                },
                result = self.reap_shutdown.recv() => {
                    debug!("Connection recv reap signal!");

                    if result {
                        let mut workers = self.workers.write().await;
                        workers.remove_connection(&self.connection).await;
                    }
                    continue;
                },
                _ = self.shutdown.recv() => {
                    debug!("Connection recv shutdown signal!");
                    let mut workers = self.workers.write().await;
                    workers.remove_connection(&self.connection).await;
                    return Ok(());
                }
            };
        }

        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}

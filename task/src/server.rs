use tokio::net::{TcpListener, TcpStream};
use std::sync::atomic::{Ordering, AtomicI64};
use chrono::{DateTime, Utc};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use log::*;
use tokio::sync::RwLock;
use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use serde_json::error::Error;
use bytes::Bytes;

use crate::store::RedisStore;
use crate::connection::Connection;
use crate::shutdown::Shutdown;

#[derive(Debug)]
struct ServerRunStats {
    connections: AtomicI64,
    started_at: DateTime<Utc>
}

#[derive(Debug)]
pub struct ServerOpts {
    pub bind_host: String,
    pub bind_port: u32,
}

#[derive(Debug)]
struct Listener<'a> {
    opts: ServerOpts,
    store: RedisStore,
    listener: TcpListener,
    workers: Arc<RwLock<Workers<'a>>>,
    stats: ServerRunStats,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

#[derive(Debug)]
pub (crate) struct Handler {
    store: RedisStore,
    connection: Connection,
    limit_connections: Arc<Semaphore>,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

#[derive(Serialize, Deserialize,Debug)]
pub enum WorkerState {
    Running,
    Quit,
    Terminate
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ClientData {
    hostname: String,
    wid: Option<String>,
    pid: i64,
    labels: Option<Vec<String>>,
    started_at: Option<i64>, //timestmap
    last_heartbeat: Option<i64>, // timestamp
    state: Option<WorkerState>,
    
}

impl ClientData {
    pub fn encode(&self) -> Result<Bytes, Error> {
        Ok(Bytes::copy_from_slice(serde_json::to_string(self)?.as_bytes()))
    }

    pub fn decode(buf: &[u8]) -> Result<ClientData, Error> {
        let mut data: ClientData = serde_json::from_str(&String::from_utf8_lossy(buf))?;

        data.started_at = Some(Utc::now().timestamp());
        data.last_heartbeat = Some(Utc::now().timestamp());
        data.state = Some(WorkerState::Running);

        Ok(data)
    }    
}

#[derive(Debug)]
pub (crate) struct Workers<'a> {
    heartbeats: HashMap<String, &'a ClientData>
}

impl<'a> Workers<'a> {
    fn new() -> Workers<'a> {
        Workers { heartbeats: HashMap::new() }
    }
}

const MAX_CONNECTIONS: usize = 250;

pub async fn run(opts: ServerOpts, store: RedisStore, shutdown: impl Future) -> Result<(), Box<dyn std::error::Error>> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let listener = TcpListener::bind(format!("{}:{}", opts.bind_host, opts.bind_port)).await?;
    let workers = Workers::new();
    let mut server = Listener {
        opts,
        stats: ServerRunStats {
            connections: AtomicI64::new(0),
            started_at: Utc::now()
        },
        workers: Arc::new(RwLock::new(workers)),
        listener,
        store,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                error!("failed to accept by {:?}", err);
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }

    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

impl<'a> Listener<'a> {
    async fn accept(&mut self) -> Result<TcpStream, Box<dyn std::error::Error>> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => {
                    debug!("Server stat: {:?}", self.stats);
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
    
    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error + '_>> {
        loop {
            self.limit_connections.acquire().await.forget();

            let socket = self.accept().await?;
            let mut conn = Connection::new(socket);

            info!("Recv conn {:?}", conn);
            match conn.handshake().await {
                Ok(client_data) => {
                    conn.set_client_data(client_data);
                    if let Some(data) = conn.client_data() {
                        info!("Add worker {:?}", data);
                    }
                },
                Err(_e) => {
                    self.stats.connections.fetch_sub(1, Ordering::SeqCst);
                    drop(conn);
                    continue;
                }
            };

            
            let mut handler = Handler {
                store: self.store.clone(),
                connection: conn,
                limit_connections: self.limit_connections.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!("connection error by {:?}", err);
                }

            });
        }
    }
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() {
            let r = tokio::select! {
                frame = self.connection.read_frame() => {
                    match frame? {
                        Some(frame) => {
                            debug!("Frame {:?}", frame);
                            frame
                        },
                        None => {
                            return Ok(());
                        }
                    }
                },
                _ = self.shutdown.recv() => {
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

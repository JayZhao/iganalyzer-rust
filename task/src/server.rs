use tokio::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicI64;
use chrono::{DateTime, Utc};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use log::*;

use crate::store::RedisStore;
use crate::connection::Connection;
use crate::shutdown::Shutdown;

struct ServerRunStats {
    connections: AtomicI64,
    started_at: DateTime<Utc>
}

pub struct ServerOpts {
    pub bind_host: String,
    pub bind_port: u32,
}

struct Listener {
    opts: ServerOpts,
    store: RedisStore,
    listener: TcpListener,
    stats: ServerRunStats,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

#[derive(Debug)]
struct Handler {
    store: RedisStore,
    connection: Connection,
    limit_connections: Arc<Semaphore>,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

const MAX_CONNECTIONS: usize = 250;

pub async fn run(opts: ServerOpts, store: RedisStore, shutdown: impl Future) -> Result<(), Box<dyn std::error::Error>> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let listener = TcpListener::bind(format!("{}:{}", opts.bind_host, opts.bind_port)).await?;
    let mut server = Listener {
        opts,
        stats: ServerRunStats {
            connections: AtomicI64::new(0),
            started_at: Utc::now()
        },
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
                println!("failed to accept by {:?}", err);
            }
        }
        _ = shutdown => {
            println!("shutting down");
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

impl Listener {
    async fn accept(&mut self) -> Result<TcpStream, Box<dyn std::error::Error>> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
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
    
    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("accepting inbound connections");

        loop {
            self.limit_connections.acquire().await.forget();

            let socket = self.accept().await?;

            let mut conn = Connection::new(socket);

            match conn.handshake().await {
                Ok(true) => {
                },
                Ok(false) => {
                    drop(conn);
                    continue;
                },
                Err(e) => {
                    println!("{:?}", e);
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
                    println!("connection error by {:?}", err);
                }
            });
        }
    }
}

impl Handler {
    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        while !self.shutdown.is_shutdown() {
            tokio::select! {
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            }
        }

        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        self.limit_connections.add_permits(1);
    }
}

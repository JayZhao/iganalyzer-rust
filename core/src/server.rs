use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

use crate::connection::Connection;

#[derive(Debug)]
struct Listener {
    listener: TcpListener,
}

#[derive(Debug)]
struct Handler {
    connection: Connection,
}

pub async fn run(listener: TcpListener) -> crate::Result<()> {
    Ok(())
}

impl Listener {
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            let socket = self.accept().await?;

            let mut handler = Handler {
                connection: Connection::new(socket),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
            });
        }
    }

    async fn accept(&mut self) -> crate::Result<TcpStream> {
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
}

impl Handler {
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        Ok(())
    }
}

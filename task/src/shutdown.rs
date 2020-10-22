use log::*;
use std::net::SocketAddr;
use tokio::sync::broadcast;

#[derive(Debug)]
pub struct Shutdown {
    shutdown: bool,
    notify: broadcast::Receiver<String>,
    wid: Option<String>,
}

impl Shutdown {
    pub fn new(notify: broadcast::Receiver<String>, wid: Option<String>) -> Shutdown {
        Shutdown {
            shutdown: false,
            notify,
            wid,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    pub async fn recv(&mut self) {
        if self.shutdown {
            return;
        }

        if let Some(wid) = &self.wid {
            match self.notify.recv().await {
                Ok(ref saddr) => {
                    if saddr == wid {
                        self.shutdown = true;
                    }
                }
                Err(e) => {
                    error!("Shutdown failed {:?}", e);
                }
            }
        } else {
            let _ = self.notify.recv().await;
            self.shutdown = true;
        }
    }
}

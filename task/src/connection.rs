use std::sync::Arc;
use crate::frame::Frame;
use crate::server::{ClientData, Workers};
use bytes::{Buf, BytesMut};
use chrono::{DateTime, Utc};
use std::net::Shutdown;
use log::*;
use std::io::Cursor;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::time::{self, Duration};
use tokio::sync::RwLock;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    workers: Arc<RwLock<Workers>>,
    pub wid: Option<String>
}

impl Connection {
    pub fn new(socket: TcpStream, workers: Arc<RwLock<Workers>>) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
            workers,
            wid: None
        }
    }

    pub fn set_wid(&mut self, wid: Option<String>) {
        self.wid = wid;
    }

    pub fn close(&self) -> io::Result<()> {
        self.stream.get_ref().shutdown(Shutdown::Both)
    }

    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            let mut buf = Cursor::new(&self.buffer[..]);

            if let Ok(()) = Frame::check(&mut buf) {
                if let Ok(frame) = Frame::parse(&mut buf) {
                    info!("Recv frame: {:?}", frame.to_string());

                    let len = buf.position();
                    self.buffer.advance(len as usize);

                    return Ok(Some(frame));
                }
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    pub async fn start(&mut self) -> crate::Result<ClientData> {
        self.say_hi().await?;
        let client_data = self.client_from_hello().await?;
        Ok(client_data)
    }

    pub(crate) async fn handshake(&mut self) -> crate::Result<ClientData> {
        let mut delay = time::delay_for(Duration::from_millis(5000));
        loop {
            tokio::select! {
                _ = &mut delay => {
                    self.send_err("Handshake timeout".into()).await?;
                    return Err("Handshake timeout".into());
                },
                r = self.start() => {
                    match r {
                        Ok(client_data) => {
                            return Ok(client_data)
                        },
                        Err(e) => {
                            self.send_err(e.to_string().into()).await?;
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn say_hi(&mut self) -> crate::Result<()> {
        self.stream.write_all("+HI\r\n".as_bytes()).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub(crate) async fn say_ok(&mut self) -> crate::Result<()> {
        self.stream.write_all("+OK\r\n".as_bytes()).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub(crate) async fn send_int(&mut self, val: i64) -> crate::Result<()> {
        self.stream
            .write_all(format!(":{}\r\n", val).as_bytes())
            .await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub(crate) async fn send_err(&mut self, err: crate::Error) -> crate::Result<()> {
        self.stream
            .write_all(format!("-ERR {:?}\r\n", err.to_string()).as_bytes())
            .await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub(crate) async fn send_msg(&mut self, msg: &str) -> crate::Result<()> {
        if msg == "" {
            self.stream
                .write_all(format!("{}\r\n", msg).as_bytes())
                .await?;
        } else {
            self.stream
                .write_all(format!("${}\r\n", msg.len()).as_bytes())
                .await?;
            self.stream
                .write_all(format!("{}\r\n", msg).as_bytes())
                .await?;
        }

        self.stream.flush().await?;

        Ok(())
    }

    pub(crate) async fn client_from_hello(&mut self) -> crate::Result<ClientData> {
        match self.read_frame().await {
            Ok(Some(frame)) => {
                if let Some((cmd, payload)) = frame.to_string()?.split_once(' ') {
                    if cmd == "HELLO" {
                        let data = match ClientData::decode(payload.as_bytes()) {
                            Ok(data) => data,
                            Err(e) => {
                                return Err("Handshake failed, invalid client data".into());
                            }
                        };
                        self.say_ok().await?;

                        return Ok(data);
                    } else {
                        return Err("Handshake failed, invalid client data".into());
                    }
                } else {
                    return Err("Handshake failed".into());
                }
            }

            Ok(None) => return Err("Invalid connection".into()),
            Err(e) => return Err(e.into()),
        }
    }
}

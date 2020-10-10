use bytes::{Buf, BytesMut};
use std::io::Cursor;
use std::rc::Weak;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::time::{self, Duration};
use log::*;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;
use crate::frame::Frame;
use crate::server::ClientData;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
    client_data: Option<ClientData>
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
            client_data: None
        }
    }

    pub fn set_client_data(&mut self, client_data: ClientData) {
        self.client_data = Some(client_data);
    }

    pub fn client_data(&self) -> Option<&ClientData> {
        self.client_data.as_ref()
    }

    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            let mut buf = Cursor::new(&self.buffer[..]);

            if let Ok(()) = Frame::check(&mut buf) {
                if let Ok(frame) =  Frame::parse(&mut buf) {
                    println!("Recv frame: {:?}", frame.to_string());
                    
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

    pub (crate) async fn handshake(&mut self) -> crate::Result<ClientData>  {
        let mut delay = time::delay_for(Duration::from_millis(5000));
        loop {
            tokio::select! {
                _ = &mut delay => {
                    self.send_msg("Handshake timeout").await?;
                    return Err("Handshake timeout".into());
                },
                r = self.start() => {
                    match r {
                        Ok(client_data) => {
                            return Ok(client_data)
                        },
                        Err(e) => {
                            self.send_msg(&e.to_string()).await?;
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    pub (crate) async fn say_hi(&mut self) -> crate::Result<()> {
        self.stream.write_all("+HI\r\n".as_bytes()).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub (crate) async fn send_msg(&mut self, msg: &str) -> crate::Result<()> {
        self.stream.write_all(format!("{}\r\n", msg).as_bytes()).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub (crate) async fn client_from_hello(&mut self) -> crate::Result<ClientData> {
        match self.read_frame().await {
            Ok(Some(frame)) => {
                let cmds: Vec<&str> = frame.to_string()?.split(' ').collect();
                if cmds[0] == "HELLO" {

                    let data = match ClientData::decode(cmds[1].as_ref()) {
                        Ok(data) => {
                            data
                        },
                        Err(e) => {
                            return Err("Handshake failed, invalid client data".into());
                        }
                    };
                    self.stream.write_all(b"+OK\r\n").await?;
                    self.stream.flush().await?;

                    return Ok(data);
                } else {
                    return Err("Handshake failed".into());
                }
            },
            
            Ok(None) => {
                return Err("Invalid connection".into())
            },
            Err(e) => {
                return Err(e.into())
            }
        }
    }
}



use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tokio::time::{self, Duration};

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,

    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub async fn _handshake(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.send_hello().await?;
        self.client_from_hello().await?;
        self.send_err_msg("Handshade failed").await?;
        Ok(())
    }

    pub (crate) async fn handshake(&mut self) -> Result<bool, Box<dyn std::error::Error>>  {

        let mut delay = time::delay_for(Duration::from_millis(50));
        loop {
            tokio::select! {
                _ = &mut delay => {
                    println!("operation timed out");
                    self.send_err_msg("Handshade timeout").await?;
                    return Ok(false);
                }
                _ = self._handshake() => {
                    println!("operation completed");
                    return Ok(true);
                }
            }
        }
    }

    pub (crate) async fn send_hello(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.stream.write_all("HELLO\r\n".as_bytes()).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub (crate) async fn send_err_msg(&mut self, err_msg: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.stream.write_all(format!("ERROR {}\r\n", err_msg).as_bytes()).await?;
        self.stream.flush().await?;

        Ok(())
    }

    pub (crate) async fn client_from_hello(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if 0 == self.stream.read_buf(&mut self.buffer).await? {
            if self.buffer.is_empty() {
                return Ok(());
            } else {
                return Err("connection reset by peer".into());
            }
        }
        
        match get_line(&mut Cursor::new(&self.buffer[..])) {
            Ok(line) => {
                println!("{:?}", String::from_utf8_lossy(line));

                let line = String::from_utf8_lossy(line);
                let cmds: Vec<&str> = line.split(' ').collect();

                match cmds[0] {
                    "HELLO" => {
                    },
                    _ => {
                        return Err("handshake failed".into());
                    }
                }
            },
            Err(_e) => {
                return Err("invalid data".into());
            }
        };
        
        Ok(())
    }
 }


pub enum Error {
    Incomplete,
    Other,
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    let start = src.position() as usize;
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            src.set_position((i + 2) as u64);
            return Ok(&src.get_ref()[start..i]);
        }
    }

    Err(Error::Incomplete)
}

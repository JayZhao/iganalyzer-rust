use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;


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
}

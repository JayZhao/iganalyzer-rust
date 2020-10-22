use tokio::net::TcpStream;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::Framed;

const HEADER_LEN: usize = 4;

#[derive(Debug)]
pub struct Connection {
    framed: Framed<TcpStream, LengthDelimitedCodec>,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        let codec = LengthDelimitedCodec::builder()
            .little_endian()
            .length_field_length(HEADER_LEN)
            .new_codec();
        let framed = Framed::new(socket, codec);

        Connection { framed }
    }
}

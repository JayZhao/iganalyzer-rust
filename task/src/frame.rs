use std::io::Cursor;
use std::fmt;
use std::str::Utf8Error;
use bytes::Bytes;
// use crate::command::COMMAND_SET;

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}

#[derive(Debug)]
pub struct Frame {
    pub bytes: Bytes
}


impl Frame {
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        if src.get_ref().len() == 0 {
            return Err(Error::Incomplete)
        }

        Ok(())
    }
    
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        let line = Frame::get_line(src)?;
        Ok(Frame{bytes: Bytes::copy_from_slice(line)})
    }

    pub fn to_string(&self) -> Result<&str, Error> {
        Ok(std::str::from_utf8(&self.bytes)?)
    }

    pub fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
        let start = src.position() as usize;
        let end = src.get_ref().len() - 1;
        
        for i in start..end {
            if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
                src.set_position((i + 2) as u64);
                return Ok(&src.get_ref()[start..i]);
            }
        }
        
        src.set_position(0);

        Err(Error::Incomplete)
    }
}

impl fmt::Display for Frame {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "invalid msg")?;
        Ok(())
    }
}

impl From<String> for Error {
    fn from(src: String) -> Error {
        Error::Other(src.into())
    }
}

impl From<&str> for Error {
    fn from(src: &str) -> Error {
        src.to_string().into()
    }
}

impl From<Utf8Error> for Error {
    fn from(_src: Utf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}

use bb8_redis::bb8::RunError;
use bb8_redis::redis::RedisError;
use serde_json;
use std::error::Error;
use std::fmt;
use std::num::ParseFloatError;
use url::ParseError;

use crate::frame;

#[derive(Debug)]
pub enum TaskError {
    InvalidUrl(String),
    Bb8Err(String),
    RedisErr(String),
    QueueEmpty(String),
    JobErr(String),
    KeyInvalid(String),
    IoError(String),
    JsonInvalid(String),
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskError::InvalidUrl(err) => write!(f, "Invalid URL: {}", err),
            TaskError::Bb8Err(err) => write!(f, "BB8 Err: {}", err),
            TaskError::RedisErr(err) => write!(f, "Redis Err: {}", err),
            TaskError::QueueEmpty(err) => write!(f, "Queue {} empty", err),
            TaskError::JobErr(err) => write!(f, "Job {}", err),
            TaskError::KeyInvalid(err) => write!(f, "Key invalid {}", err),
            TaskError::JsonInvalid(err) => write!(f, "Json invalid: {}", err),
            TaskError::IoError(err) => write!(f, "IO error: {:?}", err),
        }
    }
}

impl Error for TaskError {}

impl From<std::io::Error> for Box<TaskError> {
    fn from(io_err: std::io::Error) -> Box<TaskError> {
        let fmt = format!("{}", io_err);
        
        Box::new(TaskError::IoError(fmt))
    }
}

impl From<ParseError> for Box<TaskError> {
    fn from(parse_err: ParseError) -> Box<TaskError> {
        let fmt = format!("{}", parse_err);
        
        Box::new(TaskError::InvalidUrl(fmt))
    }
}

impl From<ParseFloatError> for Box<TaskError> {
    fn from(parse_err: ParseFloatError) -> Box<TaskError> {
        let fmt = format!("{}", parse_err);

        Box::new(TaskError::JsonInvalid(fmt))
    }
}

impl From<serde_json::error::Error> for Box<TaskError> {
    fn from(parse_err: serde_json::error::Error) -> Box<TaskError> {
        let fmt = format!("{}", parse_err);
        
        Box::new(TaskError::JsonInvalid(fmt))
    }
}

impl<E> From<RunError<E>> for Box<TaskError>
where
    E: std::error::Error + 'static,
{
    fn from(bb8_err: RunError<E>) -> Box<TaskError> {
        let fmt = format!("{}", bb8_err);
        
        Box::new(TaskError::Bb8Err(fmt))
    }
}

impl From<RedisError> for Box<TaskError> {
    fn from(redis_err: RedisError) -> Box<TaskError> {
        let fmt = format!("{}", redis_err);

        Box::new(TaskError::RedisErr(fmt))
    }
}


impl From<frame::Error> for Box<TaskError> {
    fn from(frame_err: frame::Error) -> Box<TaskError> {
        let fmt = format!("{}", frame_err);

        Box::new(TaskError::IoError(fmt))
    }
}


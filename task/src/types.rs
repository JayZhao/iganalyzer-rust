use std::error::Error;
use std::fmt;
use url::ParseError;
use bb8_redis::bb8::RunError;
use bb8_redis::redis::RedisError;

#[derive(Debug)]
pub enum RedisStoreError {
    InvalidUrl(String),
    Bb8Err(String),
    RedisErr(String),
    QueueEmpty(String),
    JobErr(String),
    KeyInvalid(String),
}

impl fmt::Display for RedisStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RedisStoreError::InvalidUrl(err) => {
                write!(f, "Invalid URL: {}", err)
            },
            RedisStoreError::Bb8Err(err) => {
                write!(f, "BB8 Err: {}", err)
            },
            RedisStoreError::RedisErr(err) => {
                write!(f, "Redis Err: {}", err)
            },
            RedisStoreError::QueueEmpty(err) => {
                write!(f, "Queue {} empty", err)
            },
            RedisStoreError::JobErr(err) => {
                write!(f, "Job {}", err)
            },
            RedisStoreError::KeyInvalid(err) => {
                write!(f, "Key invalid {}", err)
            }
        }
    }
}

impl Error for RedisStoreError {}

impl From<ParseError> for RedisStoreError {
    fn from(parse_err: ParseError) -> RedisStoreError {
        let fmt = format!("{}", parse_err);

        RedisStoreError::InvalidUrl(fmt)
    }
}

impl<E> From<RunError<E>> for RedisStoreError
where E: std::error::Error + 'static,
{
    fn from(bb8_err: RunError<E>) -> RedisStoreError {
        let fmt = format!("{}", bb8_err);

        RedisStoreError::Bb8Err(fmt)
    }
        
}

impl From<RedisError> for RedisStoreError {
    fn from(redis_err: RedisError) -> RedisStoreError {
        let fmt = format!("{}", redis_err);

        RedisStoreError::RedisErr(fmt)
    }
}


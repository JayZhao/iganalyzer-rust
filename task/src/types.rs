use url::ParseError;
use bb8_redis::bb8::RunError;
use bb8_redis::redis::RedisError;

pub enum RedisStoreError {
    InvalidUrl(String),
    Bb8Err(String),
    RedisErr(String)
}

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


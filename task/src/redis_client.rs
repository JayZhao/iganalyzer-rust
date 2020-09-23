use std::concat;
use bb8_redis::{
    bb8,
    redis::{cmd, Cmd, FromRedisValue, ToRedisArgs},
    RedisConnectionManager, RedisPool
};
use url::Url;

use crate::types::RedisStoreError;

#[derive(Debug)]
pub struct RedisClientConfig {
    host: String,
    port: u32,
    db: u8
}

impl RedisClientConfig {
    pub fn new(host: String, port: u32, db: u8) -> RedisClientConfig {
        RedisClientConfig {
            host, port, db
        }
    }

    fn to_url(&self) -> Result<Url, RedisStoreError>  {
        Ok(Url::parse(&format!("redis://{}:{}/{}", self.host, self.port, self.db))?)
    }
}

#[derive(Debug, Clone)]
pub struct RedisClient {
    pool: RedisPool
}

impl RedisClient {
    pub async fn new(config: RedisClientConfig) -> Result<RedisClient, RedisStoreError> {
        let url = config.to_url()?;
        let manager = RedisConnectionManager::new(url)?;
        let pool = RedisPool::new(bb8::Pool::builder().build(manager).await?);
        
        Ok(RedisClient { pool })
    }

    pub async fn execute<T: FromRedisValue, V: ToRedisArgs + Clone>(&self, command: &str, args: Option<&[V]>) -> Result<T, RedisStoreError> {
        let mut conn = self.pool.get().await?;
        let conn = conn.as_mut().unwrap();
        let mut query = cmd(command);

        if let Some(args) = args {
            for arg in args {
                query.arg(arg.clone());
            }
        }
        
        Ok(query.query_async(conn).await?)
    }

    pub async fn info(&self) -> Result<String, RedisStoreError> {
        let mut conn = self.pool.get().await?;
        let conn = conn.as_mut().unwrap();
        let query = cmd("info");

        Ok(query.query_async(conn).await?)
    }
}



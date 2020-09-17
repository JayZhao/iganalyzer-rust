use bb8_redis::{
    bb8,
    redis::{cmd, FromRedisValue},
    RedisConnectionManager, RedisPool
};
use url::Url;

use crate::types::RedisStoreError;

pub struct RedisClientConfig {
    host: String,
    port: u32,
    db: u8
}

impl RedisClientConfig {
    fn new(host: String, port: u32, db: u8) -> RedisClientConfig {
        RedisClientConfig {
            host, port, db
        }
    }

    fn to_url(&self) -> Result<Url, RedisStoreError>  {
        Ok(Url::parse(&format!("redis://{}:{}/{}", self.host, self.port, self.db))?)
    }
}

pub struct RedisClient {
    config: RedisClientConfig,
    pool: RedisPool
}

impl RedisClient {
    async fn new(config: RedisClientConfig) -> Result<RedisClient, RedisStoreError> {
        let url = config.to_url()?;
        let manager = RedisConnectionManager::new(url)?;
        let pool = RedisPool::new(bb8::Pool::builder().build(manager).await?);
        
        Ok(RedisClient { config, pool })
    }

    async fn execute<T: FromRedisValue>(&self, command: &str) -> Result<T, RedisStoreError> {
        let mut conn = self.pool.get().await?;
        let conn = conn.as_mut().unwrap();

        Ok(cmd(command).query_async(conn).await?)
    }
}



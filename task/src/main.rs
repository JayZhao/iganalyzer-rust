#![feature(str_split_once)]
use crate::types::TaskError;
pub type Error = Box<TaskError>;

pub type Result<T> = std::result::Result<T, Error>;

pub mod client;
pub mod command;
pub mod connection;
pub mod frame;
pub mod manager;
pub mod redis_client;
pub mod server;
pub mod shutdown;
pub mod store;
pub mod task_runner;
pub mod types;
pub mod util;
pub mod working;

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let config = redis_client::RedisClientConfig::new("localhost".into(), 6379, 0);

    let client = redis_client::RedisClient::new(config).await?;

    let mut store = store::RedisStore::new("core".into(), client).await;

    store.init_queue().await;

    server::run(
        server::ServerOpts {
            bind_host: "0.0.0.0".into(),
            bind_port: 9000,
        },
        store,
    )
    .await?;

    Ok(())
}

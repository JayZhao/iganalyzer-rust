pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T> = std::result::Result<T, Error>;

pub mod types;
pub mod redis_client;
pub mod store;
pub mod client;
pub mod util;
pub mod frame;
pub mod connection;
pub mod command;
pub mod server;
pub mod shutdown;

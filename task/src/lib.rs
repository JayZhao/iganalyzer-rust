#![feature(str_split_once)]

pub type Error = Box<dyn std::error::Error + Send + Sync>;

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

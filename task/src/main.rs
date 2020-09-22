use std::error::Error;
use bytes::Bytes;

mod types;
mod redis_client;
mod store;
mod client;
mod util;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = redis_client::RedisClientConfig::new("localhost".into(), 6379, 0);

    let client = redis_client::RedisClient::new(config).await?;


    let result = client.execute::<Option<String>, String>("PING", None).await?;


    println!("{:?}", result);

    println!("{:?}", client.info().await?);

    let store = store::RedisStore::new("core".into(), client).await;


    let n = util::utc_now();

    println!("{:?}", n);

    let s = util::parse_time(&n);
    println!("{:?}", s);


    let mut job = client::job::Job::new("test", Bytes::new(), Bytes::new());

    job.at = Some(util::utc_now());


    if let Some(scheduled) = store.get_scheduled().await {
        println!("----------------{:?}", scheduled.size().await);

        scheduled.add(job).await?;
    }

    
    Ok(())
}

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

    let store = store::RedisStore::new("core".into(), client).await;

    let mut job = client::job::Job::new("test", Vec::new(), Vec::new());

    job.at = Some(util::utc_now());


    if let Some(scheduled) = store.get_scheduled().await {
        scheduled.add(job.clone()).await?;
        
        let r = scheduled.get(&format!("{}|{}", &job.at.unwrap(), &job.jid)).await?;
        let r = scheduled.get(&r.unwrap().key()?).await?;

        // scheduled.find("*", |(index, job)| { println!("{:?} {:?}", index, job)}).await?;
        
        // let a = scheduled.page(0, 2, |(index, job)| { println!("{:?} {:?}", index, job)}).await?;

        scheduled.rem(1600867599.0458748, "Wzl3rd75Lu5jv7Em".into()).await?;
        let a = scheduled.each(|(index, job)| { println!("{:?} {:?}", index, job)}).await?;

        println!("{:?}", a);
        
    }
    
    
    Ok(())
}

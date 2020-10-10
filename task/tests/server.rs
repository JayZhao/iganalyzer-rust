use std::error::Error;
use tokio::signal;

use task::types;
use task::redis_client;
use task::store;
use task::client;
use task::util;
use task::connection;
use task::server;
use task::shutdown;
use task::command;
use task::frame;

#[tokio::test]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let config = redis_client::RedisClientConfig::new("localhost".into(), 6379, 0);

    let client = redis_client::RedisClient::new(config).await?;

    let store = store::RedisStore::new("core".into(), client).await;

    let mut job = client::job::Job::new("test", Vec::new(), Vec::new());

    job.at = Some(util::utc_now());


    server::run(server::ServerOpts{bind_host: "0.0.0.0".into(), bind_port: 9000}, store, signal::ctrl_c()).await?;
    // if let Some(scheduled) = store.get_scheduled().await {
        // scheduled.add(job.clone()).await?;
        
        // let r = scheduled.get(&format!("{}|{}", &job.at.unwrap(), &job.jid)).await?;
        // let r = scheduled.get(&r.unwrap().key()?).await?;

        // scheduled.find("*", |(index, job)| { println!("{:?} {:?}", index, job)}).await?;
        
        // let a = scheduled.page(0, 2, |(index, job)| { println!("{:?} {:?}", index, job)}).await?;

        // scheduled.rem(1600867599.0458748, "Wzl3rd75Lu5jv7Em".into()).await?;
    //scheduled.remove_before("2020-09-24T02:37:36.656845+00:00", 1, |_, _| {}).await?;

    //let a = scheduled.each(|(index, job)| { println!("{:?} {:?}", index, job)}).await?;
        // println!("{:?}", a);
        
//}
    
    
    Ok(())
}

mod followers_plus;
#[allow(dead_code, unused_imports, non_snake_case)]
mod task_generated;
#[allow(dead_code, unused_imports, non_snake_case)]
mod rocksdb_client;
use flatbuffers::get_root;
use task_generated::fp::{FBAccount, FBMedia};
use rocksdb::IteratorMode;

fn main() {
    let db = rocksdb_client::open_db("/Users/huangdezhou/fpserver.rocksdb");
    let iter = db.iterator(IteratorMode::Start); 
    for (key, _value) in iter {
        let r = db.get(&key).unwrap().unwrap();
        let skey = String::from_utf8_lossy(&key);
        println!("Saw {:?}", skey);

        match &skey[0..1] {
            "A" => {
                let account = get_root::<FBAccount>(&r);
                println!("Account ID: {:?}", account.id());
                println!("Account username: {:?}", account.username());
                println!("Account fullname: {:?}", account.fullName());
            },
            "M" => {
                let media = get_root::<FBMedia>(&r);
                println!("Media ID: {:?}", media.id());
                println!("Media Code: {:?}", media.mediaCode())
            },
            _ => {
                println!("Invalid");
            }
        }
    }
}


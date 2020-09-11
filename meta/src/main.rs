mod server;
mod connection;
mod followers_plus;
#[allow(dead_code, unused_imports, non_snake_case)]
mod task_generated;
#[allow(dead_code, unused_imports, non_snake_case)]
mod rocksdb_client;

mod account;
mod media;
use flatbuffers::get_root;
use task_generated::fp::{FBAccount, FBMedia, FBComment};
use rocksdb::{DB, Direction, IteratorMode, Options, ReadOptions};
use cpp::cpp;
use std::ffi::CString;

use std::os::raw::c_char;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

cpp!{{
    #include <iostream>
    #include "yas/serialize.hpp"
    #include "yas/std_types.hpp"
    #include <stdlib.h>
    #include <string.h>
    constexpr size_t flags = yas::binary | yas::mem | yas::no_header;
}}

const PATH: &str = "/Users/huangdezhou/fpserver.rocksdb";

fn main() {
    // let db = rocksdb_client::open_db(PATH);
    //let mut opts = ReadOptions::default();
    //opts.set_readahead_size(10485760); // 10mb

    //let iter = db.iterator_opt(IteratorMode::Start, opts);
    //let r = db.get("M2364526358243339241");

    let db = rocksdb_client::open_db(PATH);
    let mut iter = db.raw_iterator();

    // Iterate all keys from the start in lexicographic order
    //iter.seek_to_first();

    let mut count = 0;
    iter.seek_to_first();
    while iter.valid() {
        println!("{:?}", String::from_utf8_lossy(&iter.key().unwrap()));
        count = count + 1;
        iter.next();
    }

    println!("{:?}", iter.valid());
    println!("{:?}", iter.status());
    /*
    println!("{:?}", r);
    let mut count = 0;

    for (key, value) in iter {
        // println!("----------{:?}", String::from_utf8_lossy(&key));
        count = count + 1;
        println!("{:?}", key[0] as char);
        println!("{:?}", count);
        match key[0] as char {
            'A' => {
                let account = get_root::<FBAccount>(&value);
                //println!("Account ID: {:?}", account.id());
            },
            'M' => {
                let media = get_root::<FBMedia>(&value);
                println!("Media ID: {:?}", media.id());
                println!("Media Code: {:?}", media.mediaCode())
            },
            'C' => {
                let comment_ptr = value.as_ptr();
                let size = value.len();
                let s = CString::new("").unwrap();
                let mut t: u64 = 0;
                println!("________________________________________");
                let r = unsafe {
                    let mut rs = s.into_raw();
                    let r = cpp!([comment_ptr as "const char *", size as "size_t", mut rs as "char*", mut t as "uint64_t"] -> *mut c_char as "char*" {
                        std::tuple<uint64_t, std::string> tmp;
                        yas::load<flags>(
                            yas::intrusive_buffer{
                                comment_ptr,
                                size
                            },
                        tmp);
                        auto ss = std::get<1>(tmp);
                        rs = strdup(ss.c_str());
                        t = std::get<0>(tmp);

                        return rs;
                    });
                    CString::from_raw(r).into_string()
                };

                println!("Comment: {:?}", r.unwrap());
                println!("Comment Time: {:?}", t);

            },
            _ => {
                println!("Invalid");
            }
        }
    }

    println!("-------------------- {:?}", count);
     */
    println!("-------------------- {:?}", count);
    let _ = DB::destroy(&Options::default(), PATH);

}


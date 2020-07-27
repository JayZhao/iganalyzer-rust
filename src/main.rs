mod followers_plus;
#[allow(dead_code, unused_imports, non_snake_case)]
mod task_generated;
#[allow(dead_code, unused_imports, non_snake_case)]
mod rocksdb_client;
use flatbuffers::get_root;
use task_generated::fp::{FBAccount, FBMedia, FBComment};
use rocksdb::IteratorMode;
use cpp::cpp;
use std::ffi::CString;

use std::os::raw::c_char;


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
    let db = rocksdb_client::open_db(PATH);
    let iter = db.iterator(IteratorMode::Start); 
    for (key, value) in iter {
        let value = db.get(&key).unwrap().unwrap();
        let skey = String::from_utf8_lossy(&key);
        println!("Saw {:?}", skey);

        match &skey[0..1] {
            "A" => {
                let account = get_root::<FBAccount>(&value);
                println!("Account ID: {:?}", account.id());
            },
            "M" => {
                let media = get_root::<FBMedia>(&value);
                println!("Media ID: {:?}", media.id());
                println!("Media Code: {:?}", media.mediaCode())
            },
            "C" => {
                let comment_ptr = value.as_ptr();
                let size = value.len();
                let s = CString::new("").unwrap();
                let mut t: u64 = 0;
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
}


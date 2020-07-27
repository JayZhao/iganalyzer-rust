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

fn main() {
    let db = rocksdb_client::open_db("/Users/huangdezhou/fpserver.rocksdb");
    let iter = db.iterator(IteratorMode::Start); 
    for (key, value) in iter {
        // let r = db.get(&key).unwrap().unwrap();
        let skey = String::from_utf8_lossy(&key);
        //println!("Saw {:?}", skey);

        match &skey[0..1] {
            "A" => {
                let account = get_root::<FBAccount>(&value);
                println!("Account ID: {:?}", account.id());
            },
            "M" => {
                let media = get_root::<FBMedia>(&value);
                // println!("Media ID: {:?}", media.id());
                // println!("Media Code: {:?}", media.mediaCode())
            },
            "C" => {
                let comment_ptr = value.as_ptr();
                let size = value.len();
                let s = CString::new("hello").unwrap();
                let r = unsafe {
                    let mut rs = s.into_raw();
                    let r = cpp!([comment_ptr as "const char *", size as "size_t", mut rs as "char*"] -> *mut c_char as "char*" {
                        std::tuple<uint64_t, std::string> tmp;
                        yas::load<flags>(
                            yas::intrusive_buffer{
                                comment_ptr,
                                size
                            },
                        tmp);
                        std::cout << std::get<1>(tmp).c_str() << std::endl;
                                     auto ss = std::get<1>(tmp);
                                     rs = strdup(ss.c_str());
                        return rs;
                        });
                            CString::from_raw(r).into_string()
                    };

                         println!("{:?}", r);

            },
            _ => {
                println!("Invalid");
            }
        }
    }
}


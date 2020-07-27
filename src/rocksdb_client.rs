use flatbuffers::{get_root, FlatBufferBuilder};
use rocksdb::{DB, SliceTransform, DBCompressionType, Options, DBCompactionStyle, MergeOperands};
use crate::task_generated::fp::{FBAccount, FBMedia,  FBAccountArgs, FBMediaArgs, FBURLVersion, FBURLVersionArgs};

    
static BUFFER_SIZE: usize = 1024 * 1024;

pub fn config_db() -> Options {
    let mut opts = Options::default();
    opts.set_optimize_filters_for_hits(true);
    let transform = SliceTransform::create_noop();
    opts.set_prefix_extractor(transform);
    opts.set_memtable_prefix_bloom_ratio(0.02);
    opts.create_if_missing(true);
    opts.set_enable_pipelined_write(true);
    opts.set_write_buffer_size(512 << 20);
    opts.set_max_write_buffer_number(16);
    opts.set_min_write_buffer_number_to_merge(4);
    opts.set_max_bytes_for_level_base(2048 << 20);
    opts.set_max_bytes_for_level_multiplier(10.0);
    opts.set_target_file_size_base(256 << 20);
    opts.set_max_open_files(1);
    opts.set_max_bytes_for_level_base(512 * 1024 * 1024);
    opts.set_compaction_style(DBCompactionStyle::Level);
    opts.set_level_zero_file_num_compaction_trigger(10);
    opts.set_level_zero_slowdown_writes_trigger(48);
    opts.set_level_zero_stop_writes_trigger(56);
    opts.increase_parallelism(4);

    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_max_background_compactions(4);
    opts.set_skip_stats_update_on_db_open(true);
    
    opts.set_merge_operator("FPMergeOperator", concat_merge, None);

    opts
}

pub fn open_db(path: &str) -> DB {
    let opts = config_db();
    DB::open(&opts, path).unwrap()
}

fn concat_merge(new_key: &[u8],
                existing_val: Option<&[u8]>,
                operands: &mut MergeOperands)
                -> Option<Vec<u8>> {

    let mut builder = FlatBufferBuilder::new_with_capacity(BUFFER_SIZE);
    
    match existing_val {
        Some(existing_val) => {
            if !existing_val.is_empty() {
                if (&operands).size_hint().0 != 0 {
                    match String::from_utf8(new_key.into()) {
                        Ok(new_key) => {
                            match &new_key[0..1] {
                                "A" => {
                                    let old_account = get_root::<FBAccount>(existing_val);
                                    for op in operands {
                                        let new_account = get_root::<FBAccount>(op);

                                        if old_account.countTime() == 0 {
                                            if old_account.username() != new_account.username() ||
                                                old_account.fullName() != new_account.fullName() ||
                                                old_account.avatarURLString() != new_account.avatarURLString() ||
                                                old_account.isPrivate() != new_account.isPrivate()
                                            {
                                                let builder = &mut builder;
                                                let username = Some(builder.create_string(new_account.username().unwrap()));
                                                let fullName = Some(builder.create_string(new_account.fullName().unwrap()));
                                                let avatarURLString = Some(builder.create_string(new_account.avatarURLString().unwrap()));
                                                let account = FBAccount::create(builder,
                                                                  &FBAccountArgs {
                                                                      id: old_account.id(),
                                                                      username,
                                                                      fullName,
                                                                      avatarURLString,
                                                                      isPrivate: new_account.isPrivate(),
                                                                      mediaCount: old_account.mediaCount(),
                                                                      followingCount: old_account.followingCount(),
                                                                      followerCount: old_account.followerCount(),
                                                                      countTime: old_account.countTime()
                                                                  }
                                                );
                                                builder.finish(account, None);
                                                let buf = builder.finished_data();
                                                return Some(buf.to_vec());
                                            }

                                        } else if new_account.countTime() <= old_account.countTime() {

                                            return Some((*existing_val).to_vec())
                                        }
                                    }
                                },
                                "M" => {
                                    let old_media = get_root::<FBMedia>(existing_val);

                                    if old_media.createTime() != 0 {
                                        for op in operands {
                                            let new_media = get_root::<FBMedia>(op);

                                            if new_media.createTime() != 0 {
                                                if (old_media.filterType() != 0 ||
                                                    old_media.emojis().is_some() ||
                                                    old_media.hashtags().is_some()) &&
                                                    (new_media.filterType() == 0 &&
                                                     new_media.emojis().is_none() &&
                                                     new_media.hashtags().is_none()) {
                                                        if let Some(old_owner) = old_media.owner() {
                                                            if let Some(new_owner) = new_media.owner() {
                                                                {
                                                                    if let Some(old_username) = old_owner.username() {
                                                                        if let Some(new_username) = new_owner.username() {
                                                                            if old_username.len() == 0 && new_username.len() > 0 {
                                                                                let builder = &mut builder;
                                                                                let mediaCode = Some(builder.create_string(old_media.mediaCode().unwrap()));
                                                                                let id = old_media.id();
                                                                                let mediaType = old_media.mediaType();
                                                                                let createTime = old_media.createTime();
                                                                                let likeCount = old_media.likeCount();
                                                                                let commentCount = old_media.commentCount();
                                                                                let viewCount = old_media.viewCount();
                                                                                let _old_owner = old_media.owner();
                                                                                let imageURLVersions = match old_media.imageURLVersions() {
                                                                                    Some(imageURLVersions) => {
                                                                                        let mut r = vec![];

                                                                                        for imageURLVersion in imageURLVersions {
                                                                                            let urlString = Some(builder.create_string(imageURLVersion.urlString().unwrap()));
                                                                                            r.push(FBURLVersion::create(builder, &FBURLVersionArgs {
                                                                                                width: imageURLVersion.width(),
                                                                                                height: imageURLVersion.height(),
                                                                                                urlString
                                                                                            }));
                                                                                        }
                                                                                        Some(builder.create_vector(&r[..]))
                                                                                    },
                                                                                    None => None
                                                                                };
                                                                                let videoURLVersions = match old_media.videoURLVersions() {
                                                                                    Some(videoURLVersions) => {
                                                                                        let mut r = vec![];

                                                                                        for videoURLVersion in videoURLVersions {
                                                                                            let urlString = Some(builder.create_string(videoURLVersion.urlString().unwrap()));
                                                                                            r.push(FBURLVersion::create(builder, &FBURLVersionArgs {
                                                                                                width: videoURLVersion.width(),
                                                                                                height: videoURLVersion.height(),
                                                                                                urlString
                                                                                            }));
                                                                                        }

                                                                                        Some(builder.create_vector(&r[..]))
                                                                                    },
                                                                                    None => None
                                                                                };
                                                                                let isStory = old_media.isStory();
                                                                                let isIGTV = old_media.isIGTV(); 
                                                                                let filterType = old_media.filterType();
                                                                                let emojis = match old_media.emojis() {
                                                                                    Some(emojis) => {
                                                                                        let mut r = vec![];
                                                                                        for emoji in emojis {
                                                                                            let emo = builder.create_string(emoji);
                                                                                            r.push(emo);
                                                                                        }
                                                                                        Some(builder.create_vector(&r[..]))
                                                                                    },
                                                                                    None => None
                                                                                };
                                                                                let hashtags = match old_media.hashtags() {
                                                                                    Some(hashtags) => {
                                                                                        let mut r = vec![];
                                                                                        for hashtag in hashtags {
                                                                                            let tag = builder.create_string(hashtag);
                                                                                            r.push(tag);
                                                                                        }
                                                                                        Some(builder.create_vector(&r[..]))
                                                                                    },
                                                                                    None => None
                                                                                };

                                                                                let usertags = match old_media.usertags() {
                                                                                    Some(usertags) => {
                                                                                        let mut r = vec![];
                                                                                        for account in usertags {
                                                                                            let username = Some(builder.create_string(account.username().unwrap()));
                                                                                            let fullName = Some(builder.create_string(account.fullName().unwrap()));
                                                                                            let avatarURLString = Some(builder.create_string(account.avatarURLString().unwrap()));

                                                                                            let account = FBAccount::create(
                                                                                                builder,
                                                                                                &FBAccountArgs {
                                                                                                    id: account.id(),
                                                                                                    username,
                                                                                                    fullName,
                                                                                                    avatarURLString,
                                                                                                    isPrivate: account.isPrivate(),
                                                                                                    mediaCount: account.mediaCount(),
                                                                                                    followingCount: account.followingCount(),
                                                                                                    followerCount: account.followerCount(),
                                                                                                    countTime: account.countTime()
                                                                                                }
                                                                                            );

                                                                                            r.push(account);
                                                                                        }
                                                                                        Some(builder.create_vector(&r[..]))
                                                                                    },
                                                                                    None => None
                                                                                };
                                                                                let account = new_media.owner().unwrap();
                                                                                let username = Some(builder.create_string(account.username().unwrap()));
                                                                                let fullName = Some(builder.create_string(account.fullName().unwrap()));
                                                                                let avatarURLString = Some(builder.create_string(account.avatarURLString().unwrap()));

                                                                                let owner = Some(FBAccount::create(
                                                                                    builder,
                                                                                    &FBAccountArgs {
                                                                                        id: account.id(),
                                                                                        username,
                                                                                        fullName,
                                                                                        avatarURLString,
                                                                                        isPrivate: account.isPrivate(),
                                                                                        mediaCount: account.mediaCount(),
                                                                                        followingCount: account.followingCount(),
                                                                                        followerCount: account.followerCount(),
                                                                                        countTime: account.countTime()
                                                                                    }
                                                                                ));
                                                                                
                                                                                let media = FBMedia::create(builder,
                                                                                    &FBMediaArgs {
                                                                                        id, mediaType, mediaCode, createTime, likeCount,
                                                                                        commentCount, viewCount, owner,
                                                                                        imageURLVersions, videoURLVersions, isStory,
                                                                                        isIGTV, filterType, emojis, hashtags, usertags
                                                                                    }
                                                                                );
                                                                                
                                                                                builder.finish(media, None);
                                                                                let buf = builder.finished_data();
                                                                                return Some(buf.to_vec());
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        } else {
                                                            return Some((*existing_val).to_vec());
                                                        }
                                                    }
                                            
                                            }
                                        }
                                    }
                                },
                                _ => {
                                    // do nothing
                                }
                            }
                        },
                        Err(_e) => {
                            // log error
                        }
                    }
                } else {
                    return Some((*existing_val).to_vec());
                }
            } 
        },
        None => {
            // do nothing
        }
    }

    existing_val.map(|val| val.to_vec())
}

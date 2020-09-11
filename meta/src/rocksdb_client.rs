use flatbuffers::{get_root, FlatBufferBuilder};
use rocksdb::{DB, SliceTransform, DBCompressionType, Options, DBCompactionStyle, MergeOperands};
use crate::task_generated::fp::{FBAccount, FBMedia, FBComment, FBAccountArgs, FBMediaArgs, FBURLVersion, FBURLVersionArgs};
use crate::account;
    
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
    let mut buf = vec![];
    
    match existing_val {
        Some(existing_val) => {
            if !existing_val.is_empty() {
                if (&operands).size_hint().0 != 0 {
                    match new_key[0] as char {
                        'A' => {
                            let mut old_account = get_root::<FBAccount>(existing_val);

                            for op in operands {
                                let new_account = get_root::<FBAccount>(op);
                                account::merge_fbaccount(&mut builder, new_account, old_account);
                                buf.clear();
                                buf.extend_from_slice(builder.finished_data());
                                old_account = get_root::<FBAccount>(&buf);
                            }
                        },
                        'M' => {
                            let mut old_media = get_root::<FBMedia>(existing_val);

                            for op in operands {
                                let new_media = get_root::<FBMedia>(op);
                                //media::merge_fbmedia(&mut builder, new_media, old_media);
                                //buf.extend_from_slice(builder.finished_data());
                                //old_media = get_root::<FBAccount>(&buf);
                            }
                            // return Some(builder.finished_data().to_vec());
                        },
                        _ => {
                            println!("Invalid data")
                        }
                    }

                }
            } else {
                return Some((*existing_val).to_vec());
            }
            
        },
        None => {
        }
    }

    if existing_val.is_none() {
        return Some(buf);
    }
    existing_val.map(|v| v.to_vec())
}


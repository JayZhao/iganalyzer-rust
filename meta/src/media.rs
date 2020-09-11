use crate::account;
use crate::task_generated::fp::{FBAccount, FBMedia, FBMediaArgs, FBURLVersion, FBURLVersionArgs};
use flatbuffers::{get_root, WIPOffset, FlatBufferBuilder};

pub fn merge_fbmedia<'a>(builder: &'a mut FlatBufferBuilder, new_media: FBMedia<'a>, old_media: FBMedia<'a>) {

    builder.reset();

    if old_media.createTime() != 0 && new_media.createTime() != 0 {
        let view_count = std::cmp::max(old_media.viewCount(), new_media.viewCount());

        if (old_media.filterType() != 0 || old_media.emojis().is_some() || old_media.hashtags().is_some()) &&
            (new_media.filterType() == 0 && new_media.emojis().is_none() && new_media.hashtags().is_none()) {
                if old_media.owner().is_none() || (old_media.owner().is_some() && old_media.owner().unwrap().username().unwrap().len() == 0)
                    && (new_media.owner().is_some() && new_media.owner().unwrap().username().unwrap().len() > 0) {
                        
                    }
            }
    }
}

pub fn copy_fbmedia<'a>(builder: &'a mut FlatBufferBuilder, old_media: FBMedia<'a>) {
    let mut account_builder = FlatBufferBuilder::new();
    let mediaCode = match old_media.mediaCode() {
        Some(code) => Some(builder.create_string(code)),
        None => None
    };
    let createTime = old_media.createTime();
    let likeCount = old_media.likeCount();
    let commentCount = old_media.commentCount();
    let viewCount = old_media.viewCount();
    let isStory = old_media.isStory();
    let isIGTV = old_media.isIGTV();
    let filterType = old_media.filterType();
    let owner = match old_media.owner() {
        Some(owner) => {
            account::copy_fbaccount(&mut account_builder, owner);
            Some(get_root::<FBAccount>(account_builder.finished_data()))
        },
        None => None
    };
    let emojis = match old_media.emojis() {
        Some(emojis) => Some(emojis),
        None => None
    };
}

pub fn copy_fbURLVersion<'a>(builder: &'a mut FlatBufferBuilder, old_url_version: FBURLVersion<'a>) {
    let width = old_url_version.width();
    let height = old_url_version.height();
    let urlString = match old_url_version.urlString() {
        Some(url) => {
            Some(builder.create_string(url))
        },
        None => None
    };

    let version = FBURLVersion::create(builder, &FBURLVersionArgs {
        width,
        height,
        urlString
    });

    builder.finish(version, None);
}

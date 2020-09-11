use crate::task_generated::fp::{FBAccount, FBAccountArgs};
use flatbuffers::{WIPOffset, FlatBufferBuilder};

pub fn merge_fbaccount<'a>(builder: &'a mut FlatBufferBuilder, new_account: FBAccount<'a>, old_account: FBAccount<'a>) {
    builder.reset();
    
    if old_account.countTime() != 0 && new_account.countTime() == 0 {
        if old_account.username() != new_account.username()
            || old_account.fullName() != new_account.fullName()
            || old_account.avatarURLString() != new_account.avatarURLString()
            || old_account.isPrivate() != new_account.isPrivate()
        {
            let username = match new_account.username() {
                Some(username) => Some(builder.create_string(username)),
                None => None
            };

            let full_name = match new_account.fullName() {
                Some(fullname) => Some(builder.create_string(fullname)),
                None => None
            };
            let avatar = match new_account.avatarURLString() {
                Some(avatar) => Some(builder.create_string(avatar)),
                None => None
            };

            let args = FBAccountArgs {
                id: old_account.id(),
                username,
                fullName: full_name,
                avatarURLString: avatar,
                isPrivate: new_account.isPrivate(),
                mediaCount: old_account.mediaCount(),
                followingCount: old_account.followingCount(),
                followerCount: old_account.followerCount(),
                countTime: old_account.countTime()
            };

            let account = FBAccount::create(builder, &args);
            builder.finish(account, None);
        } else {
            copy_fbaccount(builder, old_account);
        }
    } else {
        copy_fbaccount(builder, old_account);
    };
}

pub fn copy_fbaccount<'a>(builder: &'a mut FlatBufferBuilder, old_account: FBAccount<'a>) {
        let username = match old_account.username() {
        Some(username) => Some(builder.create_string(username)),
        None => None
    };

    let full_name = match old_account.fullName() {
        Some(fullname) => Some(builder.create_string(fullname)),
        None => None
    };
    let avatar = match old_account.avatarURLString() {
        Some(avatar) => Some(builder.create_string(avatar)),
        None => None
    };

    let args = FBAccountArgs {
        id: old_account.id(),
        username,
        fullName: full_name,
        avatarURLString: avatar,
        isPrivate: old_account.isPrivate(),
        mediaCount: old_account.mediaCount(),
        followingCount: old_account.followingCount(),
        followerCount: old_account.followerCount(),
        countTime: old_account.countTime()
    };
    
    let account = FBAccount::create(builder, &args);
    builder.finish(account, None);
}

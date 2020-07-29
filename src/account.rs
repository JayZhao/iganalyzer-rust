use crate::task_generated::fp::{FBAccount, FBAccountArgs};
use flatbuffers::{WIPOffset, FlatBufferBuilder};
pub fn merge_fbaccount<'a>(builder: &'a mut FlatBufferBuilder, new_account: FBAccount<'a>, old_account: FBAccount<'a>) -> WIPOffset<FBAccount<'a>> {
    
    builder.reset();
    
    if old_account.countTime() != 0 {
        if new_account.countTime() == 0 {
            if old_account.username() != new_account.username()
                || old_account.fullName() != new_account.fullName()
                || old_account.avatarURLString() != new_account.avatarURLString()
                || old_account.isPrivate() != new_account.isPrivate()
            {
                let username = match new_account.username() {
                    Some(username) => Some(builder.create_string(username)),
                    None => None
                };

                let fullname = match new_account.fullName() {
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
                    fullName: fullname,
                    avatarURLString: avatar,
                    isPrivate: new_account.isPrivate(),
                    mediaCount: old_account.mediaCount(),
                    followingCount: old_account.followingCount(),
                    followerCount: old_account.followerCount(),
                    countTime: old_account.countTime()
                };

                let account = FBAccount::create(builder, &args);

                return account;
            }
        } 
    }

    FBAccount::create(builder, &old_account.into())
}

static BUFFER_SIZE: usize = 1024 * 1024;

impl<'a> From<FBAccount<'a>> for FBAccountArgs<'a> {
    fn from(account: FBAccount<'a>) -> Self {
        let mut builder = FlatBufferBuilder::new_with_capacity(BUFFER_SIZE);
        
        let username = match account.username() {
            Some(username) => Some(builder.create_string(username)),
            None => None
        };

        let fullname = match account.fullName() {
            Some(fullname) => Some(builder.create_string(fullname)),
            None => None
        };
        let avatar = match account.avatarURLString() {
            Some(avatar) => Some(builder.create_string(avatar)),
            None => None
        };

        FBAccountArgs {
            id: account.id(),
            username,
            fullName: fullname,
            avatarURLString: avatar,
            isPrivate: account.isPrivate(),
            mediaCount: account.mediaCount(),
            followingCount: account.followingCount(),
            followerCount: account.followerCount(),
            countTime: account.countTime()
        }
    }

}

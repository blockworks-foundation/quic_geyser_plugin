use serde::{Deserialize, Serialize};

use crate::{
    filters::Filter,
    types::{account::Account, block_meta::BlockMeta},
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Message {
    AccountMsg(Account),
    SlotMsg(u64),
    BlockMetaMsg(BlockMeta),
    Filters(Vec<Filter>), // sent from client to server
}

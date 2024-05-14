use serde::{Deserialize, Serialize};

use crate::{
    filters::Filter,
    types::{
        account::Account,
        block_meta::{BlockMeta, SlotMeta},
    },
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Message {
    AccountMsg(Account),
    SlotMsg(SlotMeta),
    BlockMetaMsg(BlockMeta),
    Filters(Vec<Filter>), // sent from client to server
}

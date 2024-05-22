use serde::{Deserialize, Serialize};

use crate::{
    filters::Filter,
    types::{
        account::Account,
        block_meta::{BlockMeta, SlotMeta},
        transaction::Transaction,
    },
};

// current maximum message size
const MAX_MESSAGE_SIZE: u64 = 20_000_000;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum Message {
    AccountMsg(Account),
    SlotMsg(SlotMeta),
    BlockMetaMsg(BlockMeta),
    TransactionMsg(Box<Transaction>),
    Filters(Vec<Filter>), // sent from client to server
    AddStream(u64),
}

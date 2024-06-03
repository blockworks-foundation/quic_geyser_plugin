use serde::{Deserialize, Serialize};

use crate::{
    filters::Filter,
    types::{
        account::Account, block::Block, block_meta::{BlockMeta, SlotMeta}, transaction::Transaction
    },
};

// current maximum message size
pub const MAX_MESSAGE_SIZE: u64 = 20_000_000;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum Message {
    AccountMsg(Account),
    SlotMsg(SlotMeta),
    BlockMetaMsg(BlockMeta),
    TransactionMsg(Box<Transaction>),
    Filters(Vec<Filter>), // sent from client to server
    Block(Block),
}

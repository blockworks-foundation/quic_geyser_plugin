use serde::{Deserialize, Serialize};

use crate::{
    filters::Filter,
    types::{
        account::Account,
        block_meta::{BlockMeta, SlotMeta},
        connections_parameters::ConnectionParameters,
        transaction::Transaction,
    },
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Message {
    AccountMsg(Account),
    SlotMsg(SlotMeta),
    BlockMetaMsg(BlockMeta),
    TransactionMsg(Box<Transaction>),
    Filters(Vec<Filter>), // sent from client to server
    ConnectionParameters(ConnectionParameters),
}

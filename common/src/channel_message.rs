use solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey};

use crate::types::block_meta::SlotStatus;
use crate::types::{block::Block, block_meta::BlockMeta, transaction::Transaction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountData {
    pub pubkey: Pubkey,
    pub account: Account,
    pub write_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelMessage {
    Account(AccountData, Slot, bool),
    Slot(u64, u64, SlotStatus),
    BlockMeta(BlockMeta),
    Transaction(Box<Transaction>),
    Block(Block),
}

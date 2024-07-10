use solana_sdk::{
    account::Account, clock::Slot, commitment_config::CommitmentConfig, pubkey::Pubkey,
};

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
    Slot(u64, u64, CommitmentConfig),
    BlockMeta(BlockMeta),
    Transaction(Box<Transaction>),
    Block(Block),
}

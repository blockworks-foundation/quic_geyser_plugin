use serde::{Deserialize, Serialize};
use solana_transaction_status::Reward;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum SlotStatus {
    Processed = 0,
    Confirmed = 1,
    Finalized = 2,
    FirstShredReceived = 3,
    LastShredReceived = 4,
    Dead = 5,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct SlotMeta {
    pub slot: u64,
    pub parent: u64,
    pub slot_status: SlotStatus,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct BlockMeta {
    pub parent_slot: u64,
    pub slot: u64,
    pub parent_blockhash: String,
    pub blockhash: String,
    pub rewards: Vec<Reward>,
    pub block_height: Option<u64>,
    pub executed_transaction_count: u64,
    pub entries_count: u64,
    pub block_time: u64,
}

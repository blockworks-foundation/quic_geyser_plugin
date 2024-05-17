use serde::{Deserialize, Serialize};
use solana_sdk::{
    message::v0::{LoadedAddresses, Message},
    signature::Signature,
    transaction::TransactionError,
    transaction_context::TransactionReturnData,
};
use solana_transaction_status::{InnerInstructions, Rewards};

use super::slot_identifier::SlotIdentifier;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct TransactionMeta {
    pub error: Option<TransactionError>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub inner_instructions: Option<Vec<InnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub rewards: Option<Rewards>,
    pub loaded_addresses: LoadedAddresses,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct Transaction {
    pub slot_identifier: SlotIdentifier,
    pub signatures: Vec<Signature>,
    pub message: Message,
    pub is_vote: bool,
    pub transasction_meta: TransactionMeta,
    pub index: u64,
}

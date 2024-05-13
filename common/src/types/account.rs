use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

use super::slot_identifier::SlotIdentifier;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Account {
    pub slot_identifier: SlotIdentifier,
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub write_version: u64,
    pub data: Vec<u8>,
}

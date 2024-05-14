use serde::{Deserialize, Serialize};
use solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey};

use super::slot_identifier::SlotIdentifier;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Account {
    pub slot_identifier: SlotIdentifier,
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub write_version: u64,
    pub data: Vec<u8>,
}

impl Account {
    pub fn get_account_for_test(slot: Slot, data_size: usize) -> Self {
        Account {
            slot_identifier: SlotIdentifier {
                slot,
                blockhash: Hash::new_unique(),
            },
            pubkey: Pubkey::new_unique(),
            owner: Pubkey::new_unique(),
            write_version: 0,
            data: vec![178; data_size],
        }
    }
}
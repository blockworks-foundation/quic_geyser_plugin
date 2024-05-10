use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

use super::slot_identifier::SlotIdentifier;

#[derive(Serialize, Deserialize, Clone)]
pub struct Account {
    slot_identifier: SlotIdentifier,
    pubkey: Pubkey,
    data: Vec<u8>,
}
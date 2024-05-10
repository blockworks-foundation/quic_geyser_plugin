use serde::{Deserialize, Serialize};
use solana_sdk::hash::Hash;

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct SlotIdentifier {
    slot: u64,
    blockhash : Hash,
}
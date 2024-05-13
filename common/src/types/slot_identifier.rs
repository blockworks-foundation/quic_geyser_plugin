use serde::{Deserialize, Serialize};
use solana_sdk::hash::Hash;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SlotIdentifier {
    pub slot: u64,
    pub blockhash: Hash,
}

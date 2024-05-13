use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;

#[derive(Serialize, Deserialize, Clone)]
pub struct AccountFilter {
    owner: Option<Pubkey>,
    accounts: Option<HashSet<Pubkey>>,
}

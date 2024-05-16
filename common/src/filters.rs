use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use solana_sdk::{pubkey::Pubkey, signature::Signature};

use crate::message::Message;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Filter {
    Account(AccountFilter),
    Slot,
    BlockMeta,
    Transaction(Signature),
}

impl Filter {
    pub fn allows(&self, message: &Message) -> bool {
        match &self {
            Filter::Account(account) => account.allows(message),
            Filter::Slot => matches!(message, Message::SlotMsg(_)),
            Filter::BlockMeta => matches!(message, Message::BlockMetaMsg(_)),
            Filter::Transaction(signature) => {
                match message {
                    Message::TransactionMsg(transaction) => {
                        if signature == &Signature::default() {
                            // subscibe to all the signatures
                            true
                        } else {
                            // just check the first signature
                            transaction.signatures.iter().any(|x| x == signature)
                        }
                    }
                    _ => false,
                }
            }
        }
    }
}

// setting owner to 11111111111111111111111111111111 will subscribe to all the accounts
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct AccountFilter {
    pub owner: Option<Pubkey>,
    pub accounts: Option<HashSet<Pubkey>>,
}

impl AccountFilter {
    pub fn allows(&self, message: &Message) -> bool {
        if let Message::AccountMsg(account) = message {
            if let Some(owner) = self.owner {
                // check if filter subscribes to all the accounts
                if owner == Pubkey::default() || owner == account.owner {
                    return true;
                }
            }
            if let Some(accounts) = &self.accounts {
                return accounts.contains(&account.pubkey);
            }
        }
        false
    }
}

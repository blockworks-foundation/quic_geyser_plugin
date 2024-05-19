use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use solana_sdk::{pubkey::Pubkey, signature::Signature};

use crate::message::Message;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum Filter {
    Account(AccountFilter),
    AccountsAll,
    Slot,
    BlockMeta,
    Transaction(Signature),
    TransactionsAll,
}

impl Filter {
    pub fn allows(&self, message: &Message) -> bool {
        match &self {
            Filter::Account(account) => account.allows(message),
            Filter::AccountsAll => matches!(message, Message::AccountMsg(_)),
            Filter::Slot => matches!(message, Message::SlotMsg(_)),
            Filter::BlockMeta => matches!(message, Message::BlockMetaMsg(_)),
            Filter::Transaction(signature) => {
                match message {
                    Message::TransactionMsg(transaction) => {
                        // just check the first signature
                        transaction.signatures[0] == *signature
                    }
                    _ => false,
                }
            }
            Filter::TransactionsAll => matches!(message, Message::TransactionMsg(_)),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub enum MemcmpFilterData {
    Bytes(Vec<u8>),
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MemcmpFilter {
    pub offset: u64,
    pub data: MemcmpFilterData,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug)]
#[serde(rename_all = "camelCase")]
pub enum AccountFilterType {
    Datasize(u64),
    Memcmp(MemcmpFilter),
}

// setting owner to 11111111111111111111111111111111 will subscribe to all the accounts
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct AccountFilter {
    pub owner: Option<Pubkey>,
    pub accounts: Option<HashSet<Pubkey>>,
    pub filter: Option<AccountFilterType>,
}

impl AccountFilter {
    pub fn allows(&self, message: &Message) -> bool {
        if let Message::AccountMsg(account) = message {
            if let Some(owner) = self.owner {
                if owner == account.owner {
                    // to do move the filtering somewhere else because here we need to decode the account data
                    // but cannot be avoided for now, this will lag the client is abusing this filter
                    // lagged clients will be dropped
                    if let Some(filter) = &self.filter {
                        match filter {
                            AccountFilterType::Datasize(data_length) => {
                                return account.data_length == *data_length
                            }
                            AccountFilterType::Memcmp(memcmp) => {
                                if memcmp.offset > account.data_length {
                                    return false;
                                }
                                let solana_account = account.solana_account();
                                let offset = memcmp.offset as usize;
                                let MemcmpFilterData::Bytes(bytes) = &memcmp.data;

                                if solana_account.data[offset..].len() < bytes.len() {
                                    return false;
                                }
                                return solana_account.data[offset..offset + bytes.len()]
                                    == bytes[..];
                            }
                        }
                    }
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

#[cfg(test)]
mod tests {
    use solana_sdk::{account::Account as SolanaAccount, pubkey::Pubkey};

    use crate::{
        filters::{AccountFilter, AccountFilterType, MemcmpFilter},
        message::Message,
        types::{account::Account, slot_identifier::SlotIdentifier},
    };

    #[tokio::test]
    async fn test_accounts_filter() {
        let owner = Pubkey::new_unique();

        let solana_account_1 = SolanaAccount {
            lamports: 1,
            data: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            owner,
            executable: false,
            rent_epoch: 100,
        };
        let solana_account_2 = SolanaAccount {
            lamports: 2,
            data: vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
            owner,
            executable: false,
            rent_epoch: 100,
        };
        let solana_account_3 = SolanaAccount {
            lamports: 3,
            data: vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
            owner: Pubkey::new_unique(),
            executable: false,
            rent_epoch: 100,
        };

        let msg_1 = Message::AccountMsg(Account::new(
            Pubkey::new_unique(),
            solana_account_1.clone(),
            crate::compression::CompressionType::Lz4Fast(8),
            SlotIdentifier { slot: 0 },
            0,
        ));

        let msg_2 = Message::AccountMsg(Account::new(
            Pubkey::new_unique(),
            solana_account_2.clone(),
            crate::compression::CompressionType::Lz4Fast(8),
            SlotIdentifier { slot: 0 },
            0,
        ));

        let msg_3 = Message::AccountMsg(Account::new(
            Pubkey::new_unique(),
            solana_account_3.clone(),
            crate::compression::CompressionType::Lz4Fast(8),
            SlotIdentifier { slot: 0 },
            0,
        ));

        let f1 = AccountFilter {
            owner: Some(owner),
            accounts: None,
            filter: None,
        };

        assert_eq!(f1.allows(&msg_1), true);
        assert_eq!(f1.allows(&msg_2), true);
        assert_eq!(f1.allows(&msg_3), false);

        let f2 = AccountFilter {
            owner: Some(owner),
            accounts: None,
            filter: Some(AccountFilterType::Datasize(9)),
        };
        assert_eq!(f2.allows(&msg_1), false);
        assert_eq!(f2.allows(&msg_2), false);
        assert_eq!(f2.allows(&msg_3), false);

        let f3 = AccountFilter {
            owner: Some(owner),
            accounts: None,
            filter: Some(AccountFilterType::Datasize(10)),
        };
        assert_eq!(f3.allows(&msg_1), true);
        assert_eq!(f3.allows(&msg_2), true);
        assert_eq!(f3.allows(&msg_3), false);

        let f4: AccountFilter = AccountFilter {
            owner: Some(owner),
            accounts: None,
            filter: Some(AccountFilterType::Memcmp(MemcmpFilter {
                offset: 2,
                data: crate::filters::MemcmpFilterData::Bytes(vec![3, 4, 5]),
            })),
        };
        assert_eq!(f4.allows(&msg_1), true);
        assert_eq!(f4.allows(&msg_2), false);
        assert_eq!(f4.allows(&msg_3), false);

        let f5: AccountFilter = AccountFilter {
            owner: Some(owner),
            accounts: None,
            filter: Some(AccountFilterType::Memcmp(MemcmpFilter {
                offset: 2,
                data: crate::filters::MemcmpFilterData::Bytes(vec![13, 14, 15]),
            })),
        };
        assert_eq!(f5.allows(&msg_1), false);
        assert_eq!(f5.allows(&msg_2), true);
        assert_eq!(f5.allows(&msg_3), false);
    }
}

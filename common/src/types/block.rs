use serde::{Deserialize, Serialize};

use crate::compression::CompressionType;

use super::{account::Account, block_meta::BlockMeta, transaction::Transaction};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct Block {
    pub meta: BlockMeta,
    pub transactions: Vec<u8>, // compressed transaction::Transaction
    pub accounts_updated_in_block: Vec<u8>, // compressed account::Account
    pub accounts_updated_count: u64,
    pub compression_type: CompressionType,
}

impl Block {
    pub fn build(
        meta: BlockMeta,
        transactions: Vec<Transaction>,
        accounts: Vec<Account>,
        compression_type: CompressionType,
    ) -> anyhow::Result<Self> {
        let accounts_count = accounts.len() as u64;
        let transactions_binary = bincode::serialize(&transactions)?;
        let transactions_compressed = compression_type.compress(&transactions_binary);
        let accounts_binary = bincode::serialize(&accounts)?;
        let accounts_compressed = compression_type.compress(&accounts_binary);
        Ok(Self {
            meta,
            transactions: transactions_compressed,
            accounts_updated_in_block: accounts_compressed,
            accounts_updated_count: accounts_count,
            compression_type,
        })
    }

    pub fn get_transactions(&self) -> anyhow::Result<Vec<Transaction>> {
        let transactions = match self.compression_type {
            CompressionType::None => bincode::deserialize::<Vec<Transaction>>(&self.transactions)?,
            CompressionType::Lz4Fast(_) | CompressionType::Lz4(_) => {
                let data = lz4::block::decompress(&self.transactions, None)?;
                bincode::deserialize::<Vec<Transaction>>(&data)?
            }
        };
        if transactions.len() != self.meta.executed_transaction_count as usize {
            log::error!(
                "transactions vector size is not equal to expected size in meta {} != {}",
                transactions.len(),
                self.meta.executed_transaction_count
            );
        }
        Ok(transactions)
    }

    pub fn get_accounts(&self) -> anyhow::Result<Vec<Account>> {
        let accounts = match self.compression_type {
            CompressionType::None => {
                bincode::deserialize::<Vec<Account>>(&self.accounts_updated_in_block)?
            }
            CompressionType::Lz4Fast(_) | CompressionType::Lz4(_) => {
                let data = lz4::block::decompress(&self.accounts_updated_in_block, None)?;
                bincode::deserialize::<Vec<Account>>(&data)?
            }
        };
        if accounts.len() != self.accounts_updated_count as usize {
            log::error!(
                "accounts vector size is not equal to expected {} != {}",
                accounts.len(),
                self.accounts_updated_count
            );
        }
        Ok(accounts)
    }
}

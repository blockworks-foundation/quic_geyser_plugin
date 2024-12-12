use serde::{Deserialize, Serialize};

use crate::{
    filters::Filter,
    types::{
        account::Account,
        block::Block,
        block_meta::{BlockMeta, SlotMeta},
        transaction::Transaction,
    },
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub enum Message {
    AccountMsg(Account),
    SlotMsg(SlotMeta),
    BlockMetaMsg(BlockMeta),
    TransactionMsg(Box<Transaction>),
    BlockMsg(Block),
    Filters(Vec<Filter>), // sent from client to server
    Ping,
}

impl Message {
    // used by the network
    pub fn from_binary_stream(stream: &[u8]) -> Option<(Message, usize)> {
        if stream.len() < 8 {
            return None;
        }
        let size = u64::from_le_bytes(stream[0..8].try_into().unwrap()) as usize;
        if stream.len() < size + 8 {
            return None;
        }
        let message = bincode::deserialize::<Self>(&stream[8..size + 8]).unwrap();
        Some((message, size + 8))
    }

    // used by the network
    pub fn from_binary_stream_binary(stream: &[u8]) -> Option<(Vec<u8>, usize)> {
        if stream.len() < 8 {
            return None;
        }
        let size = u64::from_le_bytes(stream[0..8].try_into().unwrap()) as usize;
        if stream.len() < size + 8 {
            return None;
        }
        Some((stream[8..size + 8].to_vec(), size + 8))
    }

    pub fn to_binary_stream(&self) -> Vec<u8> {
        let binary = bincode::serialize(self).unwrap();
        let size = binary.len().to_le_bytes();
        [size.to_vec(), binary].concat()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::{rngs::ThreadRng, Rng};
    use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};

    use crate::types::{
        account::Account,
        block_meta::{BlockMeta, SlotMeta},
        slot_identifier::SlotIdentifier,
    };

    use super::Message;

    pub fn _create_random_message(rng: &mut ThreadRng) -> Message {
        let message_type = rng.gen::<u8>() % 3;

        match message_type {
            0 => Message::SlotMsg(SlotMeta {
                slot: rng.gen(),
                parent: rng.gen(),
                commitment_config: CommitmentConfig::processed(),
            }),
            1 => {
                let data_length = rng.gen_range(10..128);
                let data = (0..data_length).map(|_| rng.gen::<u8>()).collect_vec();
                Message::AccountMsg(Account {
                    slot_identifier: SlotIdentifier { slot: rng.gen() },
                    pubkey: Pubkey::new_unique(),
                    owner: Pubkey::new_unique(),
                    lamports: rng.gen(),
                    executable: rng.gen_bool(0.5),
                    rent_epoch: rng.gen(),
                    write_version: rng.gen(),
                    data,
                    compression_type: crate::compression::CompressionType::None,
                    data_length,
                })
            }
            2 => Message::BlockMetaMsg(BlockMeta {
                parent_blockhash: "jfkjahfkajfnaf".to_string(),
                parent_slot: rng.gen(),
                slot: rng.gen(),
                blockhash: "lkjsahkjhakda".to_string(),
                block_height: rng.gen(),
                rewards: vec![],
                entries_count: rng.gen(),
                executed_transaction_count: rng.gen(),
                block_time: rng.gen(),
            }),
            _ => {
                unreachable!()
            }
        }
    }

    #[test]
    pub fn check_slot_message_size() {
        let message = Message::SlotMsg(SlotMeta {
            slot: 73282,
            parent: 8392983,
            commitment_config: CommitmentConfig::finalized(),
        });
        let binary = message.to_binary_stream();
        assert_eq!(binary.len(), 32);
    }

    #[test]
    pub fn from_to_binary_stream() {
        let message = Message::SlotMsg(SlotMeta {
            slot: 73282,
            parent: 8392983,
            commitment_config: CommitmentConfig::finalized(),
        });
        let binary = message.to_binary_stream();
        let (msg_2, _) = Message::from_binary_stream(&binary).unwrap();
        assert_eq!(message, msg_2);

        let account_data = (0..1000).map(|x: u32| (x % 255) as u8).collect();
        let message_account = Message::AccountMsg(Account {
            slot_identifier: SlotIdentifier { slot: 938920 },
            pubkey: Pubkey::new_unique(),
            owner: Pubkey::new_unique(),
            lamports: 84782739,
            executable: true,
            rent_epoch: 849293,
            write_version: 9403,
            data: account_data,
            compression_type: crate::compression::CompressionType::None,
            data_length: 1000,
        });
        let binary_2 = message_account.to_binary_stream();
        let total_binary = [binary_2, binary].concat();

        assert!(Message::from_binary_stream(&total_binary[..32]).is_none());

        let (msg3, size_msg3) = Message::from_binary_stream(&total_binary).unwrap();
        assert_eq!(msg3, message_account);
        let (msg4, _) = Message::from_binary_stream(&total_binary[size_msg3..]).unwrap();
        assert_eq!(msg4, message);
    }
}

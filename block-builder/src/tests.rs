#[cfg(test)]
mod tests {
    use std::{sync::mpsc::channel, vec};

    use itertools::Itertools;
    use quic_geyser_common::{channel_message::{AccountData, ChannelMessage}, types::{block_meta::BlockMeta, slot_identifier::SlotIdentifier, transaction::{Transaction, TransactionMeta}}};
    use solana_sdk::{account::Account, hash::Hash, message::{v0::{LoadedAddresses, Message}, MessageHeader}, pubkey::Pubkey, signature::Signature};

    use crate::block_builder::start_block_building_thread;

    #[test]
    fn test_block_creation() {
        let (channelmsg_sx, cm_rx) = channel();
        let (ms_sx, msg_rx) = channel();
        start_block_building_thread(cm_rx, ms_sx, quic_geyser_common::compression::CompressionType::None);

        let acc1_pk = Pubkey::new_unique();
        let acc1 = ChannelMessage::Account(
            AccountData {
                pubkey: acc1_pk,
                account: Account {
                    lamports: 12345,
                    data: (0..100).map(|_| rand::random::<u8>()).collect_vec(),
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version: 1,
            },
            5,
        );
        let acc2 = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: Account {
                    lamports: 12345,
                    data: (0..100).map(|_| rand::random::<u8>()).collect_vec(),
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version: 1,
            },
            5,
        );

        let acc3 = ChannelMessage::Account(
            AccountData {
                pubkey: acc1_pk,
                account: Account {
                    lamports: 12345,
                    data: (0..100).map(|_| rand::random::<u8>()).collect_vec(),
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version: 2,
            },
            5,
        );

        let acc4 = ChannelMessage::Account(
            AccountData {
                pubkey: acc1_pk,
                account: Account {
                    lamports: 12345,
                    data: (0..100).map(|_| rand::random::<u8>()).collect_vec(),
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version: 0,
            },
            5,
        );
        channelmsg_sx.send(acc1.clone()).unwrap();
        channelmsg_sx.send(acc2.clone()).unwrap();
        channelmsg_sx.send(acc3.clone()).unwrap();
        channelmsg_sx.send(acc4.clone()).unwrap();

        let block_meta = BlockMeta {
            parent_slot: 4,
            slot: 5,
            parent_blockhash: Hash::new_unique().to_string(),
            blockhash: Hash::new_unique().to_string(),
            rewards: vec![],
            block_height: Some(4),
            executed_transaction_count: 2,
            entries_count: 2,
        };
        channelmsg_sx.send(ChannelMessage::BlockMeta(block_meta)).unwrap();

        let tx1 = Transaction {
            slot_identifier: SlotIdentifier {
                slot: 5
            },
            signatures: vec![Signature::new_unique()],
            message: Message{
                header: MessageHeader {
                    num_required_signatures: 1,
                    num_readonly_signed_accounts: 0,
                    num_readonly_unsigned_accounts: 0,
                },
                account_keys: vec![acc1_pk],
                recent_blockhash: Hash::new_unique(),
                instructions: vec![],
                address_table_lookups: vec![],
            },
            is_vote: false,
            transasction_meta: TransactionMeta {
                error: None,
                fee: 0,
                pre_balances: vec![],
                post_balances: vec![],
                inner_instructions: None,
                log_messages: Some(vec!["toto".to_string()]),
                rewards: None,
                loaded_addresses: LoadedAddresses {
                    writable: vec![],
                    readonly: vec![]
                },
                return_data: None,
                compute_units_consumed: Some(1234),
            },
            index: 0,
        };
    }
}
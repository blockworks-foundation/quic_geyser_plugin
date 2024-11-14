use std::{
    collections::HashMap,
    sync::mpsc::{channel, TryRecvError},
    thread::sleep,
    time::Duration,
    vec,
};

use itertools::Itertools;
use quic_geyser_common::{
    channel_message::{AccountData, ChannelMessage},
    types::{
        block_meta::BlockMeta,
        slot_identifier::SlotIdentifier,
        transaction::{Transaction, TransactionMeta},
    },
};
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    hash::Hash,
    message::{
        v0::{LoadedAddresses, Message as SolanaMessage},
        MessageHeader,
    },
    pubkey::Pubkey,
    signature::Signature,
};

use crate::block_builder::start_block_building_thread;

#[test]
fn test_block_creation_transactions_after_blockmeta() {
    let (channelmsg_sx, cm_rx) = channel();
    let (ms_sx, msg_rx) = mio_channel::channel();
    start_block_building_thread(
        cm_rx,
        ms_sx,
        quic_geyser_common::compression::CompressionType::None,
        true,
    );

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
        false,
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
        false,
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
        false,
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
        false,
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
        block_time: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::BlockMeta(block_meta.clone()))
        .unwrap();

    let tx1 = Transaction {
        slot_identifier: SlotIdentifier { slot: 5 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx1.clone())))
        .unwrap();

    let tx2 = Transaction {
        slot_identifier: SlotIdentifier { slot: 6 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx2.clone())))
        .unwrap();

    let tx3 = Transaction {
        slot_identifier: SlotIdentifier { slot: 5 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx3.clone())))
        .unwrap();

    sleep(Duration::from_millis(1));
    let block_message = msg_rx.try_recv().unwrap();
    let ChannelMessage::Block(block) = block_message else {
        unreachable!();
    };
    let transactions = block.get_transactions().unwrap();
    let accounts = block.get_accounts().unwrap();

    let hash_map_accounts: HashMap<_, _> = accounts
        .iter()
        .map(|acc| (acc.pubkey, acc.data.clone()))
        .collect();
    let accounts_sent: HashMap<_, _> = [acc2, acc3]
        .iter()
        .map(|acc| {
            let ChannelMessage::Account(acc, ..) = acc else {
                unreachable!();
            };
            (acc.pubkey, acc.account.data.clone())
        })
        .collect();
    assert_eq!(block.meta, block_meta);
    assert_eq!(transactions.len(), 2);
    assert_eq!(transactions, vec![tx1, tx3]);
    assert_eq!(hash_map_accounts, accounts_sent);
}

#[test]
fn test_block_creation_blockmeta_after_transactions() {
    let (channelmsg_sx, cm_rx) = channel();
    let (ms_sx, msg_rx) = mio_channel::channel();
    start_block_building_thread(
        cm_rx,
        ms_sx,
        quic_geyser_common::compression::CompressionType::None,
        true,
    );

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
        false,
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
        false,
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
        false,
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
        false,
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
        block_time: 0,
    };

    let tx1 = Transaction {
        slot_identifier: SlotIdentifier { slot: 5 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx1.clone())))
        .unwrap();

    let tx2 = Transaction {
        slot_identifier: SlotIdentifier { slot: 6 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx2.clone())))
        .unwrap();

    let tx3 = Transaction {
        slot_identifier: SlotIdentifier { slot: 5 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx3.clone())))
        .unwrap();

    channelmsg_sx
        .send(ChannelMessage::BlockMeta(block_meta.clone()))
        .unwrap();

    sleep(Duration::from_millis(1));
    let block_message = msg_rx.try_recv().unwrap();
    let ChannelMessage::Block(block) = block_message else {
        unreachable!();
    };
    let transactions = block.get_transactions().unwrap();
    let accounts = block.get_accounts().unwrap();

    let hash_map_accounts: HashMap<_, _> = accounts
        .iter()
        .map(|acc| (acc.pubkey, acc.data.clone()))
        .collect();
    let accounts_sent: HashMap<_, _> = [acc2, acc3]
        .iter()
        .map(|acc| {
            let ChannelMessage::Account(acc, ..) = acc else {
                unreachable!();
            };
            (acc.pubkey, acc.account.data.clone())
        })
        .collect();
    assert_eq!(block.meta, block_meta);
    assert_eq!(transactions.len(), 2);
    assert_eq!(transactions, vec![tx1, tx3]);
    assert_eq!(hash_map_accounts, accounts_sent);
}

#[test]
fn test_block_creation_incomplete_block_after_slot_notification() {
    let (channelmsg_sx, cm_rx) = channel();
    let (ms_sx, msg_rx) = mio_channel::channel();
    start_block_building_thread(
        cm_rx,
        ms_sx,
        quic_geyser_common::compression::CompressionType::None,
        true,
    );

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
        false,
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
        false,
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
        false,
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
        false,
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
        block_time: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::BlockMeta(block_meta.clone()))
        .unwrap();

    let tx1 = Transaction {
        slot_identifier: SlotIdentifier { slot: 5 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx1.clone())))
        .unwrap();

    let tx2 = Transaction {
        slot_identifier: SlotIdentifier { slot: 6 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx2.clone())))
        .unwrap();

    let tx3 = Transaction {
        slot_identifier: SlotIdentifier { slot: 5 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx3.clone())))
        .unwrap();

    sleep(Duration::from_millis(1));
    let block_message = msg_rx.try_recv().unwrap();
    let ChannelMessage::Block(block) = block_message else {
        unreachable!();
    };
    let transactions = block.get_transactions().unwrap();
    let accounts = block.get_accounts().unwrap();

    let hash_map_accounts: HashMap<_, _> = accounts
        .iter()
        .map(|acc| (acc.pubkey, acc.data.clone()))
        .collect();
    let accounts_sent: HashMap<_, _> = [acc2, acc3]
        .iter()
        .map(|acc| {
            let ChannelMessage::Account(acc, ..) = acc else {
                unreachable!();
            };
            (acc.pubkey, acc.account.data.clone())
        })
        .collect();
    assert_eq!(block.meta, block_meta);
    assert_eq!(transactions.len(), 2);
    assert_eq!(transactions, vec![tx1, tx3]);
    assert_eq!(hash_map_accounts, accounts_sent);
}

#[test]
fn test_block_creation_incomplete_slot() {
    let (channelmsg_sx, cm_rx) = channel();
    let (ms_sx, msg_rx) = mio_channel::channel();
    start_block_building_thread(
        cm_rx,
        ms_sx,
        quic_geyser_common::compression::CompressionType::None,
        true,
    );

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
        false,
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
        false,
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
        false,
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
        false,
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
        executed_transaction_count: 5,
        entries_count: 2,
        block_time: 0,
    };

    channelmsg_sx
        .send(ChannelMessage::BlockMeta(block_meta.clone()))
        .unwrap();

    let tx1 = Transaction {
        slot_identifier: SlotIdentifier { slot: 5 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx1.clone())))
        .unwrap();

    let tx2 = Transaction {
        slot_identifier: SlotIdentifier { slot: 6 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx2.clone())))
        .unwrap();
    channelmsg_sx
        .send(ChannelMessage::Slot(5, 4, CommitmentConfig::processed()))
        .unwrap();

    let tx3 = Transaction {
        slot_identifier: SlotIdentifier { slot: 5 },
        signatures: vec![Signature::new_unique()],
        message: SolanaMessage {
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
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: Some(1234),
        },
        index: 0,
    };
    channelmsg_sx
        .send(ChannelMessage::Transaction(Box::new(tx3.clone())))
        .unwrap();

    sleep(Duration::from_millis(1));
    assert_eq!(msg_rx.try_recv(), Err(TryRecvError::Empty));
    channelmsg_sx
        .send(ChannelMessage::Slot(5, 4, CommitmentConfig::finalized()))
        .unwrap();
    sleep(Duration::from_millis(1));

    let block_message = msg_rx.try_recv().unwrap();
    let ChannelMessage::Block(block) = block_message else {
        unreachable!();
    };
    let transactions = block.get_transactions().unwrap();
    let accounts = block.get_accounts().unwrap();

    let hash_map_accounts: HashMap<_, _> = accounts
        .iter()
        .map(|acc| (acc.pubkey, acc.data.clone()))
        .collect();
    let accounts_sent: HashMap<_, _> = [acc2, acc3]
        .iter()
        .map(|acc| {
            let ChannelMessage::Account(acc, ..) = acc else {
                unreachable!();
            };
            (acc.pubkey, acc.account.data.clone())
        })
        .collect();
    assert_eq!(block.meta, block_meta);
    assert_eq!(transactions.len(), 2);
    assert_eq!(transactions, vec![tx1, tx3]);
    assert_eq!(hash_map_accounts, accounts_sent);
}

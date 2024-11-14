use std::{
    collections::{BTreeMap, HashMap},
    sync::mpsc::Receiver,
};

use itertools::Itertools;
use quic_geyser_common::{
    channel_message::{AccountData, ChannelMessage},
    compression::CompressionType,
    types::{
        account::Account, block::Block, block_meta::BlockMeta, slot_identifier::SlotIdentifier,
        transaction::Transaction,
    },
};
use solana_sdk::pubkey::Pubkey;

pub fn start_block_building_thread(
    channel_messages: Receiver<ChannelMessage>,
    output: mio_channel::Sender<ChannelMessage>,
    compression_type: CompressionType,
    build_blocks_with_accounts: bool,
) {
    std::thread::spawn(move || {
        build_blocks(
            channel_messages,
            output,
            compression_type,
            build_blocks_with_accounts,
        );
    });
}

#[derive(Default)]
struct PartialBlock {
    meta: Option<BlockMeta>,
    transactions: Vec<Transaction>,
    account_updates: HashMap<Pubkey, AccountData>,
}

pub fn build_blocks(
    channel_messages: Receiver<ChannelMessage>,
    output: mio_channel::Sender<ChannelMessage>,
    compression_type: CompressionType,
    build_blocks_with_accounts: bool,
) {
    let mut partially_build_blocks = BTreeMap::<u64, PartialBlock>::new();
    while let Ok(channel_message) = channel_messages.recv() {
        match channel_message {
            ChannelMessage::Account(account_data, slot, init) => {
                if init {
                    continue;
                }

                if build_blocks_with_accounts {
                    if let Some(lowest) = partially_build_blocks.first_entry() {
                        if *lowest.key() > slot {
                            log::error!("Account update is too late the slot data has already been dispactched lowest slot: {}, account slot: {}", lowest.key(), slot);
                        }
                    }
                    // save account updates
                    let partial_block = match partially_build_blocks.get_mut(&slot) {
                        Some(pb) => pb,
                        None => {
                            partially_build_blocks.insert(slot, PartialBlock::default());
                            partially_build_blocks.get_mut(&slot).unwrap()
                        }
                    };
                    let update = match partial_block.account_updates.get(&account_data.pubkey) {
                        Some(prev_update) => prev_update.write_version < account_data.write_version,
                        None => true,
                    };
                    if update {
                        partial_block
                            .account_updates
                            .insert(account_data.pubkey, account_data);
                    }
                }
            }
            ChannelMessage::Slot(slot, _parent_slot, commitment) => {
                if commitment.is_finalized() {
                    // dispactch partially build blocks if not already dispatched
                    dispatch_partial_block(
                        &mut partially_build_blocks,
                        slot,
                        &output,
                        compression_type,
                    );
                }
            }
            ChannelMessage::BlockMeta(meta) => {
                let slot = meta.slot;
                if let Some(lowest) = partially_build_blocks.first_entry() {
                    if *lowest.key() > meta.slot {
                        log::error!("Blockmeta update is too late the slot data has already been dispactched lowest slot: {}, account slot: {}", lowest.key(), meta.slot);
                    }
                }
                // save account updates
                let dispatch = {
                    let partial_block = match partially_build_blocks.get_mut(&meta.slot) {
                        Some(pb) => pb,
                        None => {
                            partially_build_blocks.insert(meta.slot, PartialBlock::default());
                            partially_build_blocks.get_mut(&meta.slot).unwrap()
                        }
                    };
                    let meta_tx_count = meta.executed_transaction_count;
                    if partial_block.meta.is_some() {
                        log::error!("Block meta has already been set");
                    } else {
                        partial_block.meta = Some(meta);
                    }
                    meta_tx_count == partial_block.transactions.len() as u64
                };
                if dispatch {
                    dispatch_partial_block(
                        &mut partially_build_blocks,
                        slot,
                        &output,
                        compression_type,
                    );
                }
            }
            ChannelMessage::Transaction(transaction) => {
                let slot = transaction.slot_identifier.slot;
                if let Some(lowest) = partially_build_blocks.first_entry() {
                    if *lowest.key() > transaction.slot_identifier.slot {
                        log::error!("Transactions update is too late the slot data has already been dispactched lowest slot: {}, account slot: {}", lowest.key(), slot);
                    }
                }
                // save account updates
                let (transactions_length_in_pb, meta_transactions_count) = {
                    let partial_block = match partially_build_blocks.get_mut(&slot) {
                        Some(pb) => pb,
                        None => {
                            partially_build_blocks.insert(slot, PartialBlock::default());
                            partially_build_blocks.get_mut(&slot).unwrap()
                        }
                    };
                    partial_block.transactions.push(*transaction);

                    let meta_transactions_count = partial_block
                        .meta
                        .as_ref()
                        .map(|x| x.executed_transaction_count);

                    (
                        partial_block.transactions.len() as u64,
                        meta_transactions_count,
                    )
                };

                if Some(transactions_length_in_pb) == meta_transactions_count {
                    // check if all transactions are taken into account
                    dispatch_partial_block(
                        &mut partially_build_blocks,
                        slot,
                        &output,
                        compression_type,
                    );
                }
            }
            ChannelMessage::Block(_) => {
                unreachable!()
            }
        }
    }
}

fn dispatch_partial_block(
    partial_blocks: &mut BTreeMap<u64, PartialBlock>,
    slot: u64,
    output: &mio_channel::Sender<ChannelMessage>,
    compression_type: CompressionType,
) {
    if let Some(dispatched_partial_block) = partial_blocks.remove(&slot) {
        let Some(meta) = dispatched_partial_block.meta else {
            log::error!(
                "Block was dispactched without any meta data/ cannot dispatch the block {slot}"
            );
            return;
        };
        let transactions = dispatched_partial_block.transactions;
        if transactions.len() != meta.executed_transaction_count as usize {
            log::error!(
                "for block at slot {slot} transaction size mismatch {}!={}",
                transactions.len(),
                meta.executed_transaction_count
            );
        }
        let accounts = dispatched_partial_block
            .account_updates
            .iter()
            .map(|(pubkey, account_data)| {
                let data_length = account_data.account.data.len() as u64;
                Account {
                    slot_identifier: SlotIdentifier { slot },
                    pubkey: *pubkey,
                    owner: account_data.account.owner,
                    lamports: account_data.account.lamports,
                    executable: account_data.account.executable,
                    rent_epoch: account_data.account.rent_epoch,
                    write_version: account_data.write_version,
                    data: account_data.account.data.clone(),
                    compression_type: CompressionType::None,
                    data_length,
                }
            })
            .collect_vec();
        match Block::build(meta, transactions, accounts, compression_type) {
            Ok(block) => {
                log::info!("Dispatching block for slot {}", slot);
                output.send(ChannelMessage::Block(block)).unwrap();
            }
            Err(e) => {
                log::error!("block building failed because of error: {e}")
            }
        }
    }
}

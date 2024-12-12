use std::{
    collections::BTreeMap,
    fmt::Display,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
    thread::sleep,
    time::{Duration, Instant, SystemTime},
};

use clap::Parser;
use cli::Args;
use quic_geyser_client::non_blocking::client::Client;
use quic_geyser_common::{filters::Filter, types::connections_parameters::ConnectionParameters};
use solana_rpc_client::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;

pub mod cli;

// to  create a config json
// use std::net::{Ipv4Addr, SocketAddrV4};
// use quic_geyser_plugin::config::{CompressionParameters, Config, ConfigQuicPlugin, QuicParameters};
// use serde_json::json;
// let config = Config {
//     libpath: "temp".to_string(),
//     quic_plugin: ConfigQuicPlugin {
//         address: std::net::SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 10800)),
//         quic_parameters: QuicParameters {
//             max_number_of_streams_per_client: 1024,
//             recieve_window_size: 1_000_000,
//             connection_timeout: 600,
//         },
//         compression_parameters: CompressionParameters {
//             compression_type: quic_geyser_common::compression::CompressionType::Lz4Fast(8),
//         },
//         number_of_retries: 100,
//     },
// };
// let config_json = json!(config);
//println!("{}", config_json);

struct Stats<T: Ord + Display + Default + Copy> {
    container: BTreeMap<T, usize>,
    count: usize,
}

impl<T: Ord + Display + Default + Copy> Stats<T> {
    pub fn new() -> Self {
        Self {
            container: BTreeMap::new(),
            count: 0,
        }
    }

    pub fn add_value(&mut self, value: &T) {
        self.count += 1;
        match self.container.get_mut(value) {
            Some(size) => {
                *size += 1;
            }
            None => {
                self.container.insert(*value, 1);
            }
        }
    }

    pub fn print_stats(&self, name: &str) {
        if self.count > 0 {
            let p50_index = self.count / 2;
            let p75_index = self.count * 3 / 4;
            let p90_index = self.count * 9 / 10;
            let p95_index = self.count * 95 / 100;
            let p99_index = self.count * 99 / 100;

            let mut p50_value = None::<T>;
            let mut p75_value = None::<T>;
            let mut p90_value = None::<T>;
            let mut p95_value = None::<T>;
            let mut p99_value = None::<T>;

            let mut counter = 0;
            for (value, size) in self.container.iter() {
                counter += size;
                if counter > p50_index && p50_value.is_none() {
                    p50_value = Some(*value);
                }
                if counter > p75_index && p75_value.is_none() {
                    p75_value = Some(*value);
                }
                if counter > p90_index && p90_value.is_none() {
                    p90_value = Some(*value);
                }
                if counter > p95_index && p95_value.is_none() {
                    p95_value = Some(*value);
                }
                if counter > p99_index && p99_value.is_none() {
                    p99_value = Some(*value);
                }
            }
            let max_value = self
                .container
                .last_key_value()
                .map(|x| *x.0)
                .unwrap_or_default();
            println!(
                "stats {} : p50={}, p75={}, p90={}, p95={}, p99={}, max:{}",
                name,
                p50_value.unwrap_or_default(),
                p75_value.unwrap_or_default(),
                p90_value.unwrap_or_default(),
                p95_value.unwrap_or_default(),
                p99_value.unwrap_or_default(),
                max_value
            );
        }
    }
}

fn blocking(args: Args, client_stats: ClientStats, break_thread: Arc<AtomicBool>) {
    println!("Connecting");
    let (client, reciever) =
        quic_geyser_blocking_client::client::Client::new(args.url, ConnectionParameters::default())
            .unwrap();
    println!("Connected");
    let mut filters = vec![Filter::Slot, Filter::BlockMeta];
    if args.blocks_instead_of_accounts {
        filters.push(Filter::BlockAll);
    } else {
        filters.push(Filter::AccountsAll);
    }

    sleep(Duration::from_millis(100));
    println!("Subscribing");
    client.subscribe(filters).unwrap();
    println!("Subscribed");

    std::thread::spawn(move || {
        let _client = client;
        while let Ok(message) = reciever.recv() {
            let message_size = message.to_binary_stream().len();
            client_stats
                .bytes_transfered
                .fetch_add(message_size as u64, std::sync::atomic::Ordering::Relaxed);
            match message {
                quic_geyser_common::message::Message::AccountMsg(account) => {
                    log::trace!("got account notification : {} ", account.pubkey);
                    client_stats
                        .account_notification
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let data_len = account.data_length as usize;
                    client_stats
                        .total_accounts_size
                        .fetch_add(account.data_length, std::sync::atomic::Ordering::Relaxed);
                    let solana_account = account.solana_account();
                    if solana_account.data.len() != data_len {
                        println!("data length different");
                        println!(
                            "Account : {}, owner: {}=={}, datalen: {}=={}, lamports: {}",
                            account.pubkey,
                            account.owner,
                            solana_account.owner,
                            data_len,
                            solana_account.data.len(),
                            solana_account.lamports
                        );
                        panic!("Wrong account data");
                    }

                    client_stats.account_slot.store(
                        account.slot_identifier.slot,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                quic_geyser_common::message::Message::SlotMsg(slot) => {
                    log::trace!("got slot notification : {} ", slot.slot);
                    client_stats
                        .slot_notifications
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if slot.commitment_config == CommitmentConfig::processed() {
                        client_stats
                            .slot_slot
                            .store(slot.slot, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                quic_geyser_common::message::Message::BlockMetaMsg(block_meta) => {
                    log::trace!("got blockmeta notification : {} ", block_meta.slot);
                    client_stats
                        .blockmeta_notifications
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    client_stats
                        .blockmeta_slot
                        .store(block_meta.slot, std::sync::atomic::Ordering::Relaxed);
                }
                quic_geyser_common::message::Message::TransactionMsg(tx) => {
                    log::trace!(
                        "got transaction notification: {}",
                        tx.signatures[0].to_string()
                    );
                    client_stats
                        .transaction_notifications
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                quic_geyser_common::message::Message::BlockMsg(block) => {
                    log::info!("got block notification of slot {}, number_of_transactions : {}, number_of_accounts: {}", block.meta.slot, block.get_transactions().unwrap().len(), block.get_accounts().unwrap().len());
                    client_stats
                        .block_notifications
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    client_stats
                        .block_slot
                        .store(block.meta.slot, std::sync::atomic::Ordering::Relaxed);
                }
                quic_geyser_common::message::Message::Filters(_) => {
                    // Not supported
                }
                quic_geyser_common::message::Message::Ping => {
                    // not supported
                }
            }
        }
        log::info!("breaking client thread");
        break_thread.store(true, std::sync::atomic::Ordering::Relaxed);
    });
}

async fn non_blocking(args: Args, client_stats: ClientStats, break_thread: Arc<AtomicBool>) {
    println!("Connecting");
    let (client, mut reciever, _tasks) = Client::new(
        args.url,
        ConnectionParameters {
            max_number_of_streams: args.number_of_streams,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    println!("Connected");

    // let mut filters = vec![Filter::Slot, Filter::BlockMeta];
    // if args.blocks_instead_of_accounts {
    //     filters.push(Filter::BlockAll);
    // } else {
    //     filters.push(Filter::AccountsAll);
    // }

    println!("Subscribing");
    client.subscribe(vec![Filter::AccountsAll]).await.unwrap();
    println!("Subscribed");

    tokio::spawn(async move {
        while let Some(message) = reciever.recv().await {
            let message_size = message.to_binary_stream().len();
            client_stats
                .bytes_transfered
                .fetch_add(message_size as u64, std::sync::atomic::Ordering::Relaxed);
            match message {
                quic_geyser_common::message::Message::AccountMsg(account) => {
                    log::trace!("got account notification : {} ", account.pubkey);
                    client_stats
                        .account_notification
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let data_len = account.data_length as usize;
                    client_stats
                        .total_accounts_size
                        .fetch_add(account.data_length, std::sync::atomic::Ordering::Relaxed);
                    let solana_account = account.solana_account();
                    if solana_account.data.len() != data_len {
                        println!("data length different");
                        println!(
                            "Account : {}, owner: {}=={}, datalen: {}=={}, lamports: {}",
                            account.pubkey,
                            account.owner,
                            solana_account.owner,
                            data_len,
                            solana_account.data.len(),
                            solana_account.lamports
                        );
                        panic!("Wrong account data");
                    }
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as u64;
                    let account_in_us = account.lamports;

                    client_stats.delay.fetch_add(
                        now.saturating_sub(account_in_us),
                        std::sync::atomic::Ordering::Relaxed,
                    );

                    client_stats.account_slot.store(
                        account.slot_identifier.slot,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                quic_geyser_common::message::Message::SlotMsg(slot) => {
                    log::trace!("got slot notification : {} ", slot.slot);
                    client_stats
                        .slot_notifications
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if slot.commitment_config == CommitmentConfig::processed() {
                        client_stats
                            .slot_slot
                            .store(slot.slot, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                quic_geyser_common::message::Message::BlockMetaMsg(block_meta) => {
                    log::trace!("got blockmeta notification : {} ", block_meta.slot);
                    client_stats
                        .blockmeta_notifications
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    client_stats
                        .blockmeta_slot
                        .store(block_meta.slot, std::sync::atomic::Ordering::Relaxed);
                }
                quic_geyser_common::message::Message::TransactionMsg(tx) => {
                    log::trace!(
                        "got transaction notification: {}",
                        tx.signatures[0].to_string()
                    );
                    client_stats
                        .transaction_notifications
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                quic_geyser_common::message::Message::BlockMsg(block) => {
                    log::info!("got block notification of slot {}, number_of_transactions : {}, number_of_accounts: {}", block.meta.slot, block.get_transactions().unwrap().len(), block.get_accounts().unwrap().len());
                    client_stats
                        .block_notifications
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    client_stats
                        .block_slot
                        .store(block.meta.slot, std::sync::atomic::Ordering::Relaxed);
                }
                quic_geyser_common::message::Message::Filters(_) => {
                    // Not supported
                }
                quic_geyser_common::message::Message::Ping => {
                    // not supported
                }
            }
        }

        break_thread.store(true, std::sync::atomic::Ordering::Relaxed);
    });
}

#[derive(Clone, Default)]
struct ClientStats {
    bytes_transfered: Arc<AtomicU64>,
    total_accounts_size: Arc<AtomicU64>,
    slot_notifications: Arc<AtomicU64>,
    account_notification: Arc<AtomicU64>,
    blockmeta_notifications: Arc<AtomicU64>,
    transaction_notifications: Arc<AtomicU64>,
    block_notifications: Arc<AtomicU64>,
    cluster_slot: Arc<AtomicU64>,
    account_slot: Arc<AtomicU64>,
    slot_slot: Arc<AtomicU64>,
    blockmeta_slot: Arc<AtomicU64>,
    block_slot: Arc<AtomicU64>,
    delay: Arc<AtomicU64>,
}

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();
    let client_stats = ClientStats::default();
    let args = Args::parse();
    let rpc_url = args.rpc_url.clone();
    let break_thread = Arc::new(AtomicBool::new(false));
    if args.blocking {
        blocking(args, client_stats.clone(), break_thread.clone());
    } else {
        non_blocking(args, client_stats.clone(), break_thread.clone()).await;
    }

    if let Some(rpc_url) = rpc_url {
        let cluster_slot = client_stats.cluster_slot.clone();
        let rpc = RpcClient::new(rpc_url);
        let mut last_slot = 0;
        std::thread::spawn(move || loop {
            sleep(Duration::from_millis(200));
            if let Ok(slot) = rpc.get_slot_with_commitment(CommitmentConfig::processed()) {
                if last_slot < slot {
                    last_slot = slot;
                    cluster_slot.store(slot, std::sync::atomic::Ordering::Relaxed);
                }
            }
        });
    }

    {
        let bytes_transfered: Arc<AtomicU64> = client_stats.bytes_transfered.clone();
        let slot_notifications = client_stats.slot_notifications.clone();
        let account_notification = client_stats.account_notification.clone();
        let blockmeta_notifications = client_stats.blockmeta_notifications.clone();
        let transaction_notifications = client_stats.transaction_notifications.clone();
        let total_accounts_size = client_stats.total_accounts_size.clone();
        let block_notifications = client_stats.block_notifications.clone();

        let cluster_slot = client_stats.cluster_slot.clone();
        let account_slot = client_stats.account_slot.clone();
        let slot_slot = client_stats.slot_slot.clone();
        let blockmeta_slot = client_stats.blockmeta_slot.clone();
        let block_slot = client_stats.block_slot.clone();
        let delay = client_stats.delay.clone();

        let mut instant = Instant::now();
        let mut bytes_transfered_stats = Stats::<u64>::new();
        let mut slot_notifications_stats = Stats::<u64>::new();
        let mut account_notification_stats = Stats::<u64>::new();
        let mut blockmeta_notifications_stats = Stats::<u64>::new();
        let mut transaction_notifications_stats = Stats::<u64>::new();
        let mut total_accounts_size_stats = Stats::<u64>::new();
        let mut block_notifications_stats = Stats::<u64>::new();
        let mut counter = 0;
        let start_instance = Instant::now();
        loop {
            counter += 1;
            tokio::time::sleep(Duration::from_secs(1) - instant.elapsed()).await;
            instant = Instant::now();
            let bytes_transfered = bytes_transfered.swap(0, std::sync::atomic::Ordering::Relaxed);
            let total_accounts_size =
                total_accounts_size.swap(0, std::sync::atomic::Ordering::Relaxed);
            let account_notification =
                account_notification.swap(0, std::sync::atomic::Ordering::Relaxed);
            let slot_notifications =
                slot_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
            let blockmeta_notifications =
                blockmeta_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
            let transaction_notifications =
                transaction_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
            let block_notifications =
                block_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
            let avg_delay_us = delay.swap(0, std::sync::atomic::Ordering::Relaxed) as f64
                / account_notification as f64;
            bytes_transfered_stats.add_value(&bytes_transfered);
            total_accounts_size_stats.add_value(&total_accounts_size);
            slot_notifications_stats.add_value(&slot_notifications);
            account_notification_stats.add_value(&account_notification);
            blockmeta_notifications_stats.add_value(&blockmeta_notifications);
            transaction_notifications_stats.add_value(&transaction_notifications);
            block_notifications_stats.add_value(&block_notifications);

            log::info!("------------------------------------------");
            log::info!(
                " DateTime : {:?}",
                instant.duration_since(start_instance).as_secs()
            );
            log::info!(" Bytes Transfered : {} Mbs/s", bytes_transfered / 1_000_000);
            log::info!(
                " Accounts transfered size (uncompressed) : {} Mbs",
                total_accounts_size / 1_000_000
            );
            log::info!(" Accounts Notified : {}", account_notification);
            log::info!(" Slots Notified : {}", slot_notifications);
            log::info!(" Blockmeta notified : {}", blockmeta_notifications);
            log::info!(" Transactions notified : {}", transaction_notifications);
            log::info!(" Blocks notified : {}", block_notifications);
            log::info!(
                " Average delay by accounts : {:.02} ms",
                avg_delay_us / 1000.0
            );

            log::info!(" Cluster Slots: {}, Account Slot: {}, Slot Notification slot: {}, BlockMeta slot: {}, Block slot: {}", cluster_slot.load(std::sync::atomic::Ordering::Relaxed), account_slot.load(std::sync::atomic::Ordering::Relaxed), slot_slot.load(std::sync::atomic::Ordering::Relaxed), blockmeta_slot.load(std::sync::atomic::Ordering::Relaxed), block_slot.load(std::sync::atomic::Ordering::Relaxed));

            if counter % 10 == 0 {
                println!("------------------STATS------------------------");
                bytes_transfered_stats.print_stats("Bytes transfered");
                total_accounts_size_stats.print_stats("Total account size uncompressed");
                slot_notifications_stats.print_stats("Slot Notifications");
                account_notification_stats.print_stats("Account notifications");
                blockmeta_notifications_stats.print_stats("Block meta Notifications");
                transaction_notifications_stats.print_stats("Transaction notifications");
                block_notifications_stats.print_stats("Block Notifications");
            }
            if break_thread.load(std::sync::atomic::Ordering::Relaxed) {
                println!("------------------STATS------------------------");
                bytes_transfered_stats.print_stats("Bytes transfered");
                total_accounts_size_stats.print_stats("Total account size uncompressed");
                slot_notifications_stats.print_stats("Slot Notifications");
                account_notification_stats.print_stats("Account notifications");
                blockmeta_notifications_stats.print_stats("Block meta Notifications");
                transaction_notifications_stats.print_stats("Transaction notifications");
                block_notifications_stats.print_stats("Block Notifications");
                break;
            }
        }
    }
}

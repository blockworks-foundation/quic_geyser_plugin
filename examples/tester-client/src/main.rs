use std::{
    collections::BTreeMap,
    fmt::Display,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
    thread::sleep,
    time::{Duration, Instant, UNIX_EPOCH},
};

use clap::Parser;
use cli::Args;
use quic_geyser_client::non_blocking::client::Client;
use quic_geyser_common::{filters::Filter, types::{block_meta::BlockMeta, connections_parameters::ConnectionParameters}};
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

async fn non_blocking(args: Args) {
    loop {
        let args = args.clone();
        log::info!("Connecting");
        let (client, mut reciever, _tasks) = Client::new(
            args.url,
            ConnectionParameters {
                max_number_of_streams: args.number_of_streams,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        log::info!("Connected");

        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut filters = vec![Filter::Slot, Filter::BlockMeta];
        filters.push(Filter::AccountsAll);

        log::info!("Subscribing");
        client.subscribe(filters).await.unwrap();
        log::info!("Subscribed");

        let mut last_slot = 0u64;
        let mut last_block_meta = Option::<BlockMeta>::None;
        let mut btree_map = BTreeMap::<u64, u128>::new();

        while let Some(message) = reciever.recv().await {
            match message {
                quic_geyser_common::message::Message::AccountMsg(account) => {
                    log::trace!("got account notification : {} ", account.pubkey);
                    let slot = account.slot_identifier.slot;
                    if slot > last_slot {
                        let system_time = std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                        btree_map.insert(slot, system_time.as_micros());
                        //println!("new slot {} notification at {}", slot, system_time.as_micros());
                        last_slot = slot;
                    }
                }
                quic_geyser_common::message::Message::SlotMsg(slot) => {
                    log::trace!("got slot notification : {} ", slot.slot);
                    let slot = slot.slot;
                    if slot > last_slot {
                        let system_time = std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                        btree_map.insert(slot, system_time.as_micros());
                        //println!("new slot {} notification at {}", slot, system_time.as_micros());
                        last_slot = slot;
                    }
                }
                quic_geyser_common::message::Message::BlockMetaMsg(block_meta) => {
                    log::trace!("got blockmeta notification : {} ", block_meta.slot);
                    let slot = block_meta.slot;
                    if slot > last_slot {
                        let system_time = std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
                        btree_map.insert(slot, system_time.as_micros());
                        //println!("new slot {} notification at {}", slot, system_time.as_micros());
                        last_slot = slot;
                    }
                    if let Some(last_block_meta) = &last_block_meta {
                        if block_meta.slot < last_block_meta.slot {
                            continue;
                        }
                        let last_first_shread_time = btree_map.get(&last_block_meta.slot).copied().unwrap();
                        let first_shread_time = btree_map.get(&slot).copied().unwrap();
                        let drift = (first_shread_time - last_first_shread_time) as i64 - 400 * 1000;
                        println!("block_meta : slot {}, current_block_time: {}, last_block_slot: {}, last_block_time:{}, observed drift : {}", block_meta.slot, first_shread_time, last_block_meta.slot, last_first_shread_time, drift);
                        while let Some((k, _)) = btree_map.first_key_value() {
                            if *k < last_block_meta.slot {
                                btree_map.pop_first();
                            } else {
                                break;
                            }
                        }
                    }
                    last_block_meta = Some(block_meta);
                }
                _ => {
                    // Not supported
                }
            }
        }
    }
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
}

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();
    let client_stats = ClientStats::default();
    let args = Args::parse();
    let rpc_url = args.rpc_url.clone();
    let jh = tokio::spawn( async move {
        non_blocking(args).await
    });

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

    let _ = jh.await.unwrap();

    // {
    //     let bytes_transfered: Arc<AtomicU64> = client_stats.bytes_transfered.clone();
    //     let slot_notifications = client_stats.slot_notifications.clone();
    //     let account_notification = client_stats.account_notification.clone();
    //     let blockmeta_notifications = client_stats.blockmeta_notifications.clone();
    //     let transaction_notifications = client_stats.transaction_notifications.clone();
    //     let total_accounts_size = client_stats.total_accounts_size.clone();
    //     let block_notifications = client_stats.block_notifications.clone();

    //     let cluster_slot = client_stats.cluster_slot.clone();
    //     let account_slot = client_stats.account_slot.clone();
    //     let slot_slot = client_stats.slot_slot.clone();
    //     let blockmeta_slot = client_stats.blockmeta_slot.clone();
    //     let block_slot = client_stats.block_slot.clone();

    //     let mut instant = Instant::now();
    //     let mut bytes_transfered_stats = Stats::<u64>::new();
    //     let mut slot_notifications_stats = Stats::<u64>::new();
    //     let mut account_notification_stats = Stats::<u64>::new();
    //     let mut blockmeta_notifications_stats = Stats::<u64>::new();
    //     let mut transaction_notifications_stats = Stats::<u64>::new();
    //     let mut total_accounts_size_stats = Stats::<u64>::new();
    //     let mut block_notifications_stats = Stats::<u64>::new();
    //     let mut counter = 0;
    //     let start_instance = Instant::now();
    //     loop {
    //         counter += 1;
    //         tokio::time::sleep(Duration::from_secs(1) - instant.elapsed()).await;
    //         instant = Instant::now();
    //         let bytes_transfered = bytes_transfered.swap(0, std::sync::atomic::Ordering::Relaxed);
    //         let total_accounts_size =
    //             total_accounts_size.swap(0, std::sync::atomic::Ordering::Relaxed);
    //         let account_notification =
    //             account_notification.swap(0, std::sync::atomic::Ordering::Relaxed);
    //         let slot_notifications =
    //             slot_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
    //         let blockmeta_notifications =
    //             blockmeta_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
    //         let transaction_notifications =
    //             transaction_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
    //         let block_notifications =
    //             block_notifications.swap(0, std::sync::atomic::Ordering::Relaxed);
    //         bytes_transfered_stats.add_value(&bytes_transfered);
    //         total_accounts_size_stats.add_value(&total_accounts_size);
    //         slot_notifications_stats.add_value(&slot_notifications);
    //         account_notification_stats.add_value(&account_notification);
    //         blockmeta_notifications_stats.add_value(&blockmeta_notifications);
    //         transaction_notifications_stats.add_value(&transaction_notifications);
    //         block_notifications_stats.add_value(&block_notifications);

    //         println!("------------------------------------------");
    //         println!(
    //             " DateTime : {:?}",
    //             instant.duration_since(start_instance).as_secs()
    //         );
    //         println!(" Bytes Transfered : {} Mbs/s", bytes_transfered / 1_000_000);
    //         println!(
    //             " Accounts transfered size (uncompressed) : {} Mbs",
    //             total_accounts_size / 1_000_000
    //         );
    //         println!(" Accounts Notified : {}", account_notification);
    //         println!(" Slots Notified : {}", slot_notifications);
    //         println!(" Blockmeta notified : {}", blockmeta_notifications);
    //         println!(" Transactions notified : {}", transaction_notifications);
    //         println!(" Blocks notified : {}", block_notifications);

    //         println!(" Cluster Slots: {}, Account Slot: {}, Slot Notification slot: {}, BlockMeta slot: {}, Block slot: {}", cluster_slot.load(std::sync::atomic::Ordering::Relaxed), account_slot.load(std::sync::atomic::Ordering::Relaxed), slot_slot.load(std::sync::atomic::Ordering::Relaxed), blockmeta_slot.load(std::sync::atomic::Ordering::Relaxed), block_slot.load(std::sync::atomic::Ordering::Relaxed));

    //         if counter % 10 == 0 {
    //             println!("------------------STATS------------------------");
    //             bytes_transfered_stats.print_stats("Bytes transfered");
    //             total_accounts_size_stats.print_stats("Total account size uncompressed");
    //             slot_notifications_stats.print_stats("Slot Notifications");
    //             account_notification_stats.print_stats("Account notifications");
    //             blockmeta_notifications_stats.print_stats("Block meta Notifications");
    //             transaction_notifications_stats.print_stats("Transaction notifications");
    //             block_notifications_stats.print_stats("Block Notifications");
    //         }
    //         if break_thread.load(std::sync::atomic::Ordering::Relaxed) {
    //             println!("------------------STATS------------------------");
    //             bytes_transfered_stats.print_stats("Bytes transfered");
    //             total_accounts_size_stats.print_stats("Total account size uncompressed");
    //             slot_notifications_stats.print_stats("Slot Notifications");
    //             account_notification_stats.print_stats("Account notifications");
    //             blockmeta_notifications_stats.print_stats("Block meta Notifications");
    //             transaction_notifications_stats.print_stats("Transaction notifications");
    //             block_notifications_stats.print_stats("Block Notifications");
    //             break;
    //         }
    //     }
    // }
}

use std::{
    sync::{atomic::AtomicU64, Arc},
    thread::sleep,
    time::{Duration, Instant},
};

use clap::Parser;
use cli::Args;
use quic_geyser_client::client::Client;
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

pub fn main() {
    let args = Args::parse();
    println!("Connecting");
    let (client, reciever) = Client::new(args.url, ConnectionParameters::default()).unwrap();
    println!("Connected");

    let bytes_transfered = Arc::new(AtomicU64::new(0));
    let total_accounts_size = Arc::new(AtomicU64::new(0));
    let slot_notifications = Arc::new(AtomicU64::new(0));
    let account_notification = Arc::new(AtomicU64::new(0));
    let blockmeta_notifications = Arc::new(AtomicU64::new(0));
    let transaction_notifications = Arc::new(AtomicU64::new(0));

    let cluster_slot = Arc::new(AtomicU64::new(0));
    let account_slot = Arc::new(AtomicU64::new(0));
    let slot_slot = Arc::new(AtomicU64::new(0));
    let blockmeta_slot = Arc::new(AtomicU64::new(0));

    if let Some(rpc_url) = args.rpc_url {
        let cluster_slot = cluster_slot.clone();
        let rpc = RpcClient::new(rpc_url);
        std::thread::spawn(move || loop {
            sleep(Duration::from_millis(100));
            let slot = rpc
                .get_slot_with_commitment(CommitmentConfig::processed())
                .unwrap();
            cluster_slot.store(slot, std::sync::atomic::Ordering::Relaxed);
        });
    }

    {
        let bytes_transfered: Arc<AtomicU64> = bytes_transfered.clone();
        let slot_notifications = slot_notifications.clone();
        let account_notification = account_notification.clone();
        let blockmeta_notifications = blockmeta_notifications.clone();
        let transaction_notifications = transaction_notifications.clone();
        let total_accounts_size = total_accounts_size.clone();

        let cluster_slot = cluster_slot.clone();
        let account_slot = account_slot.clone();
        let slot_slot = slot_slot.clone();
        let blockmeta_slot = blockmeta_slot.clone();
        std::thread::spawn(move || loop {
            sleep(Duration::from_secs(1));
            let bytes_transfered = bytes_transfered.swap(0, std::sync::atomic::Ordering::Relaxed);
            println!("------------------------------------------");
            println!(" Bytes Transfered : {}", bytes_transfered);
            println!(
                " Accounts transfered size (uncompressed) : {}",
                total_accounts_size.swap(0, std::sync::atomic::Ordering::Relaxed)
            );
            println!(
                " Accounts Notified : {}",
                account_notification.swap(0, std::sync::atomic::Ordering::Relaxed)
            );
            println!(
                " Slots Notified : {}",
                slot_notifications.swap(0, std::sync::atomic::Ordering::Relaxed)
            );
            println!(
                " Blockmeta notified : {}",
                blockmeta_notifications.swap(0, std::sync::atomic::Ordering::Relaxed)
            );
            println!(
                " Transactions notified : {}",
                transaction_notifications.swap(0, std::sync::atomic::Ordering::Relaxed)
            );

            println!(" Cluster Slots: {}, Account Slot: {}, Slot Notification slot: {}, BlockMeta slot: {} ", cluster_slot.load(std::sync::atomic::Ordering::Relaxed), account_slot.load(std::sync::atomic::Ordering::Relaxed), slot_slot.load(std::sync::atomic::Ordering::Relaxed), blockmeta_slot.load(std::sync::atomic::Ordering::Relaxed));
        });
    }

    println!("Subscribing");
    client
        .subscribe(vec![
            Filter::AccountsAll,
            Filter::TransactionsAll,
            Filter::Slot,
            Filter::BlockMeta,
        ])
        .unwrap();
    println!("Subscribed");

    let instant = Instant::now();

    while let Ok(message) = reciever.recv() {
        let message_size = bincode::serialize(&message).unwrap().len();
        bytes_transfered.fetch_add(message_size as u64, std::sync::atomic::Ordering::Relaxed);
        match message {
            quic_geyser_common::message::Message::AccountMsg(account) => {
                log::debug!("got account notification : {} ", account.pubkey);
                account_notification.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let data_len = account.data_length as usize;
                total_accounts_size
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

                account_slot.store(
                    account.slot_identifier.slot,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            quic_geyser_common::message::Message::SlotMsg(slot) => {
                log::debug!("got slot notification : {} ", slot.slot);
                slot_notifications.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                slot_slot.store(slot.slot, std::sync::atomic::Ordering::Relaxed);
            }
            quic_geyser_common::message::Message::BlockMetaMsg(block_meta) => {
                log::debug!("got blockmeta notification : {} ", block_meta.slot);
                blockmeta_notifications.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                blockmeta_slot.store(block_meta.slot, std::sync::atomic::Ordering::Relaxed);
            }
            quic_geyser_common::message::Message::TransactionMsg(tx) => {
                log::debug!(
                    "got transaction notification: {}",
                    tx.signatures[0].to_string()
                );
                transaction_notifications.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            quic_geyser_common::message::Message::Filters(_) => {
                // Not supported
            }
        }
    }
    println!(
        "Conection closed and streaming stopped in {} seconds",
        instant.elapsed().as_secs()
    );
}

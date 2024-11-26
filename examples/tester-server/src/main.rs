use clap::Parser;
use cli::Args;
use itertools::Itertools;
use quic_geyser_common::{
    channel_message::{AccountData, ChannelMessage},
    config::{CompressionParameters, ConfigQuicPlugin, QuicParameters},
    net::parse_host_port,
};
use quic_geyser_server::quic_server::QuicServer;
use rand::{thread_rng, Rng};
use solana_sdk::{account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey};
use std::time::Duration;

pub mod cli;

pub fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let config = ConfigQuicPlugin {
        address: parse_host_port(format!("[::]:{}", args.port).as_str()).unwrap(),
        log_level: "info".to_string(),
        quic_parameters: QuicParameters {
            max_number_of_streams_per_client: args.number_of_streams,
            ..Default::default()
        },
        compression_parameters: CompressionParameters {
            compression_type: quic_geyser_common::compression::CompressionType::None,
        },
        number_of_retries: 100,
        allow_accounts: true,
        allow_accounts_at_startup: false,
        enable_block_builder: false,
        build_blocks_with_accounts: false,
    };
    let quic_server = QuicServer::new(config).unwrap();
    // to avoid errors
    std::thread::sleep(Duration::from_millis(500));

    let mut slot = 4;
    let mut write_version = 1;
    let mut rand = thread_rng();
    let datas = (0..args.number_of_random_accounts)
        .map(|_| {
            let size: usize =
                rand.gen_range(args.min_account_data_size..args.max_account_data_size);
            let data = (0..size).map(|_| rand.gen::<u8>()).collect_vec();
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: Account {
                    lamports: rand.gen(),
                    data,
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version,
            }
        })
        .collect_vec();
    let sleep_time_in_nanos = 1_000_000_000 / (args.accounts_per_second / 1000) as u64;
    loop {
        slot += 1;
        quic_server
            .send_message(ChannelMessage::Slot(
                slot,
                slot - 1,
                CommitmentConfig::processed(),
            ))
            .unwrap();
        quic_server
            .send_message(ChannelMessage::Slot(
                slot - 1,
                slot - 2,
                CommitmentConfig::confirmed(),
            ))
            .unwrap();
        quic_server
            .send_message(ChannelMessage::Slot(
                slot - 2,
                slot - 3,
                CommitmentConfig::finalized(),
            ))
            .unwrap();
        for i in 1..args.accounts_per_second + 1 {
            write_version += 1;
            let data_index = 0;
            let mut account = datas.get(data_index).unwrap().clone();
            account.write_version = write_version;
            let channel_message = ChannelMessage::Account(account, slot, false);
            quic_server.send_message(channel_message).unwrap();
            if i % 1000 == 0 {
                std::thread::sleep(Duration::from_nanos(sleep_time_in_nanos));
            }
        }
    }
}

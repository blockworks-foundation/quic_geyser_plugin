use std::{
    net::SocketAddr,
    str::FromStr,
    time::{Duration, Instant},
};

use clap::Parser;
use cli::Args;
use itertools::Itertools;
use quic_geyser_common::{
    channel_message::{AccountData, ChannelMessage},
    config::{CompressionParameters, ConfigQuicPlugin, QuicParameters},
};
use quic_geyser_server::quic_server::QuicServer;
use rand::{thread_rng, Rng};
use solana_sdk::{account::Account, pubkey::Pubkey};

pub mod cli;

pub fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let config = ConfigQuicPlugin {
        address: SocketAddr::from_str(format!("0.0.0.0:{}", args.port).as_str()).unwrap(),
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
        enable_block_builder: true,
        build_blocks_with_accounts: true,
    };
    let quic_server = QuicServer::new(config).unwrap();

    let mut instant = Instant::now();
    // to avoid errors
    std::thread::sleep(Duration::from_millis(500));

    let mut slot = 1;
    let mut write_version = 1;
    let mut rand = thread_rng();
    let data = (0..args.account_data_size as usize)
        .map(|_| rand.gen::<u8>())
        .collect_vec();
    loop {
        let diff = Instant::now().duration_since(instant);
        if diff < Duration::from_secs(1) {
            std::thread::sleep(Duration::from_secs(1) - diff);
        }
        instant = Instant::now();
        slot += 1;
        for _ in 0..args.accounts_per_second {
            write_version += 1;
            let account = AccountData {
                pubkey: Pubkey::new_unique(),
                account: Account {
                    lamports: rand.gen(),
                    data: data.clone(),
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version,
            };
            let channel_message = ChannelMessage::Account(account, slot, false);
            quic_server.send_message(channel_message).unwrap();
        }
    }
}

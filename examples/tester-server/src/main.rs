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
    quic::{configure_client::DEFAULT_MAX_RECIEVE_WINDOW_SIZE, quic_server::QuicServer},
};
use rand::{thread_rng, Rng};
use solana_sdk::{account::Account, pubkey::Pubkey};

pub mod cli;

pub fn main() {
    let args = Args::parse();

    let config = ConfigQuicPlugin {
        address: SocketAddr::from_str(format!("0.0.0.0:{}", args.port).as_str()).unwrap(),
        quic_parameters: QuicParameters {
            max_number_of_streams_per_client: 1024 * 1024,
            recieve_window_size: DEFAULT_MAX_RECIEVE_WINDOW_SIZE,
            connection_timeout: 60,
        },
        compression_parameters: CompressionParameters {
            compression_type: quic_geyser_common::compression::CompressionType::None,
        },
        number_of_retries: 100,
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
        std::thread::sleep(Duration::from_secs(1) - Instant::now().duration_since(instant));
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

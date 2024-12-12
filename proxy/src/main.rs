use clap::Parser;
use cli::Args;
use quic_geyser_blocking_client::client::Client;
use quic_geyser_common::{
    channel_message::{AccountData, ChannelMessage},
    config::{CompressionParameters, ConfigQuicPlugin, QuicParameters},
    filters::Filter,
    net::parse_host_port,
    types::connections_parameters::ConnectionParameters,
};
use quic_geyser_server::quic_server::QuicServer;

pub mod cli;

pub fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let (client, message_channel) = Client::new(
        args.source_url,
        ConnectionParameters {
            max_number_of_streams: args.max_number_of_streams_per_client,
            recieve_window_size: args.recieve_window_size,
            timeout_in_seconds: args.connection_timeout,
            max_ack_delay: args.max_ack_delay,
            ack_exponent: args.ack_exponent,
            enable_gso: true,
            enable_pacing: true,
        },
    )?;

    log::info!("Subscribing");
    client.subscribe(vec![
        Filter::AccountsAll,
        Filter::TransactionsAll,
        Filter::Slot,
        Filter::BlockMeta,
        Filter::BlockAll,
    ])?;

    let quic_config = ConfigQuicPlugin {
        address: parse_host_port(format!("[::]:{}", args.port).as_str()).unwrap(),
        log_level: "info".to_string(),
        quic_parameters: QuicParameters {
            max_number_of_streams_per_client: args.max_number_of_streams_per_client,
            recieve_window_size: args.recieve_window_size,
            connection_timeout: args.connection_timeout,
            max_number_of_connections: args.max_number_of_connections,
            max_ack_delay: args.max_ack_delay,
            ack_exponent: args.ack_exponent,
            ..Default::default()
        },
        compression_parameters: CompressionParameters {
            compression_type: quic_geyser_common::compression::CompressionType::Lz4Fast(
                args.compression_speed,
            ),
        },
        number_of_retries: 100,
        allow_accounts: true,
        allow_accounts_at_startup: false,
        enable_block_builder: false,
        build_blocks_with_accounts: false,
    };

    let (server_sender, server_reciever) = std::sync::mpsc::channel::<ChannelMessage>();
    std::thread::spawn(move || {
        let quic_server = QuicServer::new(quic_config).unwrap();
        loop {
            match server_reciever.recv() {
                Ok(channel_message) => {
                    if quic_server.send_message(channel_message).is_err() {
                        log::error!("server broken");
                        break;
                    }
                }
                Err(_) => {
                    log::info!("closing server");
                    break;
                }
            }
        }
    });

    while let Ok(message) = message_channel.recv() {
        let channel_message = match message {
            quic_geyser_common::message::Message::AccountMsg(account_message) => {
                ChannelMessage::Account(
                    AccountData {
                        pubkey: account_message.pubkey,
                        account: account_message.solana_account(),
                        write_version: account_message.write_version,
                    },
                    account_message.slot_identifier.slot,
                    false,
                )
            }
            quic_geyser_common::message::Message::SlotMsg(slot_message) => ChannelMessage::Slot(
                slot_message.slot,
                slot_message.parent,
                slot_message.commitment_config,
            ),
            quic_geyser_common::message::Message::BlockMetaMsg(block_meta_message) => {
                ChannelMessage::BlockMeta(block_meta_message)
            }
            quic_geyser_common::message::Message::TransactionMsg(transaction_message) => {
                ChannelMessage::Transaction(transaction_message)
            }
            _ => {
                unreachable!()
            }
        };
        if server_sender.send(channel_message).is_err() {
            log::error!("Server stopped");
            break;
        }
    }

    Ok(())
}

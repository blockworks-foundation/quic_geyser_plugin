use std::{net::SocketAddr, str::FromStr};

use clap::Parser;
use cli::Args;
use quic_geyser_blocking_client::client::Client;
use quic_geyser_common::{
    channel_message::{AccountData, ChannelMessage},
    config::{CompressionParameters, ConfigQuicPlugin, QuicParameters},
    filters::Filter,
    types::{
        block_meta::BlockMeta, connections_parameters::ConnectionParameters,
        transaction::Transaction,
    },
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
        },
    )?;

    log::info!("Subscribing");
    client.subscribe(vec![
        Filter::AccountsAll,
        Filter::TransactionsAll,
        Filter::Slot,
        Filter::BlockMeta,
    ])?;

    let quic_config = ConfigQuicPlugin {
        address: SocketAddr::from_str(format!("0.0.0.0:{}", args.port).as_str()).unwrap(),
        log_level: "info".to_string(),
        quic_parameters: QuicParameters {
            max_number_of_streams_per_client: args.max_number_of_streams_per_client,
            recieve_window_size: args.recieve_window_size,
            connection_timeout: args.connection_timeout,
            max_number_of_connections: args.max_number_of_connections,
            max_ack_delay: args.max_ack_delay,
            ack_exponent: args.ack_exponent,
        },
        compression_parameters: CompressionParameters {
            compression_type: quic_geyser_common::compression::CompressionType::Lz4Fast(
                args.compression_speed,
            ),
        },
        number_of_retries: 100,
        allow_accounts: true,
        allow_accounts_at_startup: false,
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
                )
            }
            quic_geyser_common::message::Message::SlotMsg(slot_message) => ChannelMessage::Slot(
                slot_message.slot,
                slot_message.parent,
                slot_message.commitment_level,
            ),
            quic_geyser_common::message::Message::BlockMetaMsg(block_meta_message) => {
                ChannelMessage::BlockMeta(BlockMeta {
                    parent_slot: block_meta_message.parent_slot,
                    slot: block_meta_message.slot,
                    parent_blockhash: block_meta_message.parent_blockhash,
                    blockhash: block_meta_message.blockhash,
                    rewards: block_meta_message.rewards,
                    block_height: block_meta_message.block_height,
                    executed_transaction_count: block_meta_message.executed_transaction_count,
                    entries_count: block_meta_message.entries_count,
                })
            }
            quic_geyser_common::message::Message::TransactionMsg(transaction_message) => {
                ChannelMessage::Transaction(Box::new(Transaction {
                    slot_identifier: transaction_message.slot_identifier,
                    signatures: transaction_message.signatures,
                    message: transaction_message.message,
                    is_vote: transaction_message.is_vote,
                    transasction_meta: transaction_message.transasction_meta,
                    index: transaction_message.index,
                }))
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

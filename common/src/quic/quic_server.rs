use std::{net::UdpSocket, sync::Arc};

use crate::{
    compression::CompressionType,
    config::ConfigQuicPlugin,
    message::Message,
    plugin_error::QuicGeyserError,
    quic::{configure_server::configure_server, connection_manager::ConnectionManager},
    types::{
        account::Account as GeyserAccount,
        block_meta::{BlockMeta, SlotMeta},
        slot_identifier::SlotIdentifier,
        transaction::Transaction,
    },
};
use quinn::{Endpoint, EndpointConfig, TokioRuntime};
use solana_sdk::{
    account::Account, clock::Slot, commitment_config::CommitmentLevel, pubkey::Pubkey,
};
use tokio::{runtime::Runtime, sync::mpsc::UnboundedSender};

pub struct AccountData {
    pub pubkey: Pubkey,
    pub account: Account,
    pub write_version: u64,
}

pub enum ChannelMessage {
    Account(AccountData, Slot, bool),
    Slot(u64, u64, CommitmentLevel),
    BlockMeta(BlockMeta),
    Transaction(Box<Transaction>),
}

#[derive(Debug)]
pub struct QuicServer {
    _runtime: Runtime,
    data_channel_sender: UnboundedSender<ChannelMessage>,
}

impl QuicServer {
    pub fn new(
        runtime: Runtime,
        config: ConfigQuicPlugin,
        drop_laggers: bool,
    ) -> anyhow::Result<Self> {
        let server_config = configure_server(
            config.quic_parameters.max_number_of_streams_per_client,
            config.quic_parameters.recieve_window_size,
            config.quic_parameters.connection_timeout as u64,
        )?;
        let socket = UdpSocket::bind(config.address)?;
        let compression_type = config.compression_parameters.compression_type;

        let (data_channel_sender, mut data_channel_tx) = tokio::sync::mpsc::unbounded_channel();

        {
            runtime.spawn(async move {
                let endpoint = Endpoint::new(
                    EndpointConfig::default(),
                    Some(server_config),
                    socket,
                    Arc::new(TokioRuntime),
                )
                .unwrap();
                let retry_count = config.number_of_retries;

                let (quic_connection_manager, _jh) = ConnectionManager::new(endpoint);
                log::info!("Connection manager sucessfully started");
                while let Some(channel_message) = data_channel_tx.recv().await {
                    match channel_message {
                        ChannelMessage::Account(account, slot, is_startup) => {
                            // avoid sending messages at startup
                            if !is_startup {
                                process_account_message(
                                    quic_connection_manager.clone(),
                                    account,
                                    slot,
                                    compression_type,
                                    retry_count,
                                    drop_laggers,
                                );
                            }
                        }
                        ChannelMessage::Slot(slot, parent, commitment_level) => {
                            let message = Message::SlotMsg(SlotMeta {
                                slot,
                                parent,
                                commitment_level,
                            });
                            quic_connection_manager
                                .dispatch(message, retry_count, drop_laggers)
                                .await;
                        }
                        ChannelMessage::BlockMeta(block_meta) => {
                            let message = Message::BlockMetaMsg(block_meta);
                            quic_connection_manager
                                .dispatch(message, retry_count, drop_laggers)
                                .await;
                        }
                        ChannelMessage::Transaction(transaction) => {
                            let message = Message::TransactionMsg(transaction);
                            quic_connection_manager
                                .dispatch(message, retry_count, drop_laggers)
                                .await;
                        }
                    }
                }
                log::error!("quic server dispatch task stopped");
            });
        }

        Ok(QuicServer {
            data_channel_sender,
            _runtime: runtime,
        })
    }

    pub fn send_message(&self, message: ChannelMessage) -> Result<(), QuicGeyserError> {
        self.data_channel_sender
            .send(message)
            .map_err(|_| QuicGeyserError::MessageChannelClosed)
    }
}

fn process_account_message(
    quic_connection_manager: ConnectionManager,
    account: AccountData,
    slot: Slot,
    compression_type: CompressionType,
    retry_count: u64,
    drop_laggers: bool,
) {
    tokio::spawn(async move {
        let slot_identifier = SlotIdentifier { slot };
        let geyser_account = GeyserAccount::new(
            account.pubkey,
            account.account,
            compression_type,
            slot_identifier,
            account.write_version,
        );

        let message = Message::AccountMsg(geyser_account);
        quic_connection_manager
            .dispatch(message, retry_count, drop_laggers)
            .await;
    });
}

use std::{
    net::{IpAddr, Ipv4Addr, UdpSocket},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use agave_geyser_plugin_interface::geyser_plugin_interface::{GeyserPluginError, Result};
use quic_geyser_common::{
    compression::CompressionType,
    message::Message,
    quic::{configure_server::configure_server, connection_manager::ConnectionManager},
    types::{
        account::Account as GeyserAccount,
        block_meta::{BlockMeta, SlotMeta},
        slot_identifier::SlotIdentifier,
    },
};
use quinn::{Endpoint, EndpointConfig, TokioRuntime};
use solana_sdk::{
    account::Account, clock::Slot, commitment_config::CommitmentLevel, pubkey::Pubkey,
    signature::Keypair,
};
use tokio::{
    runtime::{Builder, Runtime},
    sync::mpsc::UnboundedSender,
};

use crate::{config::Config, plugin_error::QuicGeyserError};

pub struct AccountData {
    pub pubkey: Pubkey,
    pub account: Account,
    pub write_version: u64,
}

pub enum ChannelMessage {
    Account(AccountData, Slot, bool),
    Slot(u64, u64, CommitmentLevel),
    BlockMeta(BlockMeta),
}

#[derive(Debug)]
pub struct QuicServer {
    _runtime: Runtime,
    data_channel_sender: UnboundedSender<ChannelMessage>,
}

impl QuicServer {
    pub fn new(identity: Keypair, config: Config) -> anyhow::Result<Self> {
        let (server_config, _) = configure_server(
            &identity,
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            config
                .quic_plugin
                .quic_parameters
                .max_number_of_streams_per_client,
            config.quic_plugin.quic_parameters.recieve_window_size,
            config.quic_plugin.quic_parameters.connection_timeout as u64,
        )?;
        let socket = UdpSocket::bind(config.quic_plugin.address)?;
        let compression_type = config.quic_plugin.compression_parameters.compression_type;

        let runtime = Builder::new_multi_thread()
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
                format!("solGeyserGrpc{id:02}")
            })
            .enable_all()
            .build()
            .map_err(|error| {
                let s = error.to_string();
                log::error!("Runtime Error : {}", s);
                GeyserPluginError::Custom(Box::new(error))
            })?;

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
                let retry_count = config.quic_plugin.number_of_retries;

                let (quic_connection_manager, _jh) = ConnectionManager::new(
                    endpoint,
                    config
                        .quic_plugin
                        .quic_parameters
                        .max_number_of_streams_per_client as usize,
                );
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
                                );
                            }
                        }
                        ChannelMessage::Slot(slot, parent, commitment_level) => {
                            let message = Message::SlotMsg(SlotMeta {
                                slot,
                                parent,
                                commitment_level,
                            });
                            quic_connection_manager.dispach(message, retry_count).await;
                        }
                        ChannelMessage::BlockMeta(block_meta) => {
                            let message = Message::BlockMetaMsg(block_meta);
                            quic_connection_manager.dispach(message, retry_count).await;
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

    pub fn send_message(&self, message: ChannelMessage) -> Result<()> {
        self.data_channel_sender
            .send(message)
            .map_err(|_| GeyserPluginError::Custom(Box::new(QuicGeyserError::MessageChannelClosed)))
    }
}

fn process_account_message(
    quic_connection_manager: ConnectionManager,
    account: AccountData,
    slot: Slot,
    compression_type: CompressionType,
    retry_count: u64,
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
        quic_connection_manager.dispach(message, retry_count).await;
    });
}

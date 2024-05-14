use std::{
    net::{IpAddr, Ipv4Addr, UdpSocket},
    sync::Arc,
};

use agave_geyser_plugin_interface::geyser_plugin_interface::{GeyserPluginError, Result};
use quic_geyser_common::{
    compression::CompressionType,
    message::Message,
    quic::{configure_server::configure_server, connection_manager::ConnectionManager},
    types::{
        account::Account as GeyserAccount, block_meta::BlockMeta, slot_identifier::SlotIdentifier,
    },
};
use quinn::{Endpoint, EndpointConfig, TokioRuntime};
use solana_sdk::{account::Account, clock::Slot, pubkey::Pubkey, signature::Keypair};
use tokio::sync::mpsc::UnboundedSender;

use crate::{config::Config, plugin_error::QuicGeyserError};

pub struct AccountData {
    pub pubkey: Pubkey,
    pub account: Account,
    pub write_version: u64,
}

pub enum ChannelMessage {
    Account(AccountData, Slot, bool),
    Slot(u64),
    BlockMeta(BlockMeta),
}

#[derive(Debug)]
pub struct QuicServer {
    _quic_connection_manager: ConnectionManager,
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

        let endpoint = Endpoint::new(
            EndpointConfig::default(),
            Some(server_config),
            socket,
            Arc::new(TokioRuntime),
        )?;
        let retry_count = config.quic_plugin.number_of_retries;

        let (quic_connection_manager, _jh) = ConnectionManager::new(
            endpoint,
            config
                .quic_plugin
                .quic_parameters
                .max_number_of_streams_per_client as usize,
        );
        let (data_channel_sender, mut data_channel_tx) = tokio::sync::mpsc::unbounded_channel();

        {
            let quic_connection_manager = quic_connection_manager.clone();
            tokio::spawn(async move {
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
                        ChannelMessage::Slot(slot) => {
                            let message = Message::SlotMsg(slot);
                            quic_connection_manager.dispach(message, retry_count).await;
                        }
                        ChannelMessage::BlockMeta(block_meta) => {
                            let message = Message::BlockMetaMsg(block_meta);
                            quic_connection_manager.dispach(message, retry_count).await;
                        }
                    }
                }
            });
        }

        Ok(QuicServer {
            _quic_connection_manager: quic_connection_manager,
            data_channel_sender,
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

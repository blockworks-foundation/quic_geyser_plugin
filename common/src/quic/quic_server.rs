use std::{fmt::Debug, sync::mpsc};

use crate::{
    channel_message::ChannelMessage, config::ConfigQuicPlugin, plugin_error::QuicGeyserError,
    quic::configure_server::configure_server,
};

use super::quiche_server_loop::server_loop;
pub struct QuicServer {
    data_channel_sender: mpsc::Sender<ChannelMessage>,
    pub quic_plugin_config: ConfigQuicPlugin,
}

impl Debug for QuicServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicServer").finish()
    }
}

impl QuicServer {
    pub fn new(config: ConfigQuicPlugin) -> anyhow::Result<Self> {
        let server_config = configure_server(
            config.quic_parameters.max_number_of_streams_per_client,
            config.quic_parameters.recieve_window_size,
            config.quic_parameters.connection_timeout,
        )?;
        let socket = config.address;
        let compression_type = config.compression_parameters.compression_type;

        let (data_channel_sender, data_channel_tx) = mpsc::channel();

        let _server_loop_jh = std::thread::spawn(move || {
            if let Err(e) = server_loop(
                server_config,
                socket,
                data_channel_tx,
                compression_type,
                true,
            ) {
                panic!("Server loop closed by error : {e}");
            }
        });

        Ok(QuicServer {
            data_channel_sender,
            quic_plugin_config: config,
        })
    }

    pub fn send_message(&self, message: ChannelMessage) -> Result<(), QuicGeyserError> {
        self.data_channel_sender
            .send(message)
            .map_err(|_| QuicGeyserError::MessageChannelClosed)
    }
}

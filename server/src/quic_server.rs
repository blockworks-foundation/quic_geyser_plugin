use quic_geyser_common::{
    channel_message::ChannelMessage, config::ConfigQuicPlugin, plugin_error::QuicGeyserError,
};
use std::fmt::Debug;

use super::quiche_server_loop::server_loop;
pub struct QuicServer {
    pub data_channel_sender: mio_channel::Sender<ChannelMessage>,
    pub quic_plugin_config: ConfigQuicPlugin,
}

impl Debug for QuicServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicServer").finish()
    }
}

impl QuicServer {
    pub fn new(config: ConfigQuicPlugin) -> anyhow::Result<Self> {
        let socket = config.address;
        let compression_type = config.compression_parameters.compression_type;

        let (data_channel_sender, data_channel_tx) = mio_channel::channel();

        let _server_loop_jh = std::thread::spawn(move || {
            if let Err(e) = server_loop(
                config.quic_parameters,
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

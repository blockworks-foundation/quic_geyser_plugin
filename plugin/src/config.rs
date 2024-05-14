use std::{
    fs::read_to_string,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::Path,
};

use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use quic_geyser_common::{
    compression::CompressionType, quic::configure_client::DEFAULT_MAX_STREAMS,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub libpath: String,
    pub quic_plugin: ConfigQuicPlugin,
}

impl Config {
    fn load_from_str(config: &str) -> std::result::Result<Self, GeyserPluginError> {
        serde_json::from_str(config).map_err(|error| GeyserPluginError::ConfigFileReadError {
            msg: error.to_string(),
        })
    }

    pub fn load_from_file<P: AsRef<Path>>(file: P) -> std::result::Result<Self, GeyserPluginError> {
        let config = read_to_string(file).map_err(GeyserPluginError::ConfigFileOpenError)?;
        Self::load_from_str(&config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigQuicPlugin {
    /// Address of Grpc service.
    #[serde(default = "ConfigQuicPlugin::default_address")]
    pub address: SocketAddr,
    #[serde(default)]
    pub quic_parameters: QuicParameters,
    #[serde(default)]
    pub compression_parameters: CompressionParameters,
    #[serde(default = "ConfigQuicPlugin::default_number_of_retries")]
    pub number_of_retries: u64,
}

impl ConfigQuicPlugin {
    fn default_address() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 10800))
    }

    fn default_number_of_retries() -> u64 {
        100
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicParameters {
    pub max_number_of_streams_per_client: u32,
    pub recieve_window_size: u32,
    pub connection_timeout: u32,
}

impl Default for QuicParameters {
    fn default() -> Self {
        Self {
            max_number_of_streams_per_client: DEFAULT_MAX_STREAMS,
            recieve_window_size: 1_000_000, // 1 Mb
            connection_timeout: 10,         // 10s
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompressionParameters {
    pub compression_type: CompressionType,
}

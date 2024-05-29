use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use serde::{Deserialize, Serialize};

use crate::{
    compression::CompressionType,
    defaults::{
        DEFAULT_ACK_EXPONENT, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_MAX_ACK_DELAY,
        DEFAULT_MAX_NB_CONNECTIONS, DEFAULT_MAX_RECIEVE_WINDOW_SIZE, DEFAULT_MAX_STREAMS,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigQuicPlugin {
    #[serde(default = "ConfigQuicPlugin::default_log_level")]
    pub log_level: String,
    /// Address of Grpc service.
    #[serde(default = "ConfigQuicPlugin::default_address")]
    pub address: SocketAddr,
    #[serde(default)]
    pub quic_parameters: QuicParameters,
    #[serde(default)]
    pub compression_parameters: CompressionParameters,
    #[serde(default = "ConfigQuicPlugin::default_number_of_retries")]
    pub number_of_retries: u64,
    #[serde(default = "ConfigQuicPlugin::default_allow_accounts")]
    pub allow_accounts: bool,
    #[serde(default)]
    pub allow_accounts_at_startup: bool,
}

impl ConfigQuicPlugin {
    fn default_address() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 10800))
    }

    fn default_number_of_retries() -> u64 {
        100
    }

    fn default_log_level() -> String {
        "info".to_string()
    }

    fn default_allow_accounts() -> bool {
        true
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
pub struct QuicParameters {
    pub max_number_of_streams_per_client: u64,
    pub recieve_window_size: u64,
    pub connection_timeout: u64,
    pub max_number_of_connections: u64,
    pub max_ack_delay: u64,
    pub ack_exponent: u64,
}

impl Default for QuicParameters {
    fn default() -> Self {
        Self {
            max_number_of_streams_per_client: DEFAULT_MAX_STREAMS,
            recieve_window_size: DEFAULT_MAX_RECIEVE_WINDOW_SIZE, // 1 Mb
            connection_timeout: DEFAULT_CONNECTION_TIMEOUT,       // 10s
            max_number_of_connections: DEFAULT_MAX_NB_CONNECTIONS,
            max_ack_delay: DEFAULT_MAX_ACK_DELAY,
            ack_exponent: DEFAULT_ACK_EXPONENT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompressionParameters {
    pub compression_type: CompressionType,
}

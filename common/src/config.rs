use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

use serde::{Deserialize, Serialize};

use crate::{
    compression::CompressionType,
    defaults::{
        DEFAULT_ACK_EXPONENT, DEFAULT_CC_ALGORITHM, DEFAULT_CONNECTION_TIMEOUT,
        DEFAULT_DISCONNECT_LAGGY_CLIENTS, DEFAULT_DISCOVER_PMTU, DEFAULT_ENABLE_GSO,
        DEFAULT_ENABLE_PACING, DEFAULT_INCREMENTAL_PRIORITY, DEFAULT_MAX_ACK_DELAY,
        DEFAULT_MAX_NB_CONNECTIONS, DEFAULT_MAX_RECIEVE_WINDOW_SIZE, DEFAULT_MAX_STREAMS,
    },
};

pub fn default_true() -> bool {
    true
}

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
    #[serde(default = "default_true")]
    pub allow_accounts: bool,
    #[serde(default)]
    pub allow_accounts_at_startup: bool,
    #[serde(default)]
    pub enable_block_builder: bool,
    #[serde(default = "default_true")]
    pub build_blocks_with_accounts: bool,
}

impl ConfigQuicPlugin {
    fn default_address() -> SocketAddr {
        SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 10800, 0, 0))
    }

    fn default_number_of_retries() -> u64 {
        100
    }

    fn default_log_level() -> String {
        "info".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicParameters {
    #[serde(default = "default_max_number_of_streams_per_client")]
    pub max_number_of_streams_per_client: u64,
    #[serde(default = "default_recieve_window_size")]
    pub recieve_window_size: u64,
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout: u64,
    #[serde(default = "default_max_number_of_connections")]
    pub max_number_of_connections: u64,
    #[serde(default = "default_max_ack_delay")]
    pub max_ack_delay: u64,
    #[serde(default = "default_ack_exponent")]
    pub ack_exponent: u64,
    #[serde(default = "enable_pacing")]
    pub enable_pacing: bool,
    #[serde(default = "default_cc_algorithm")]
    pub cc_algorithm: String,
    #[serde(default = "default_incremental_priority")]
    pub incremental_priority: bool,
    #[serde(default = "default_enable_gso")]
    pub enable_gso: bool,
    #[serde(default = "default_discover_pmtu")]
    pub discover_pmtu: bool,
    #[serde(default = "default_disconnect_laggy_client")]
    pub disconnect_laggy_client: bool,
}

fn default_max_number_of_streams_per_client() -> u64 {
    DEFAULT_MAX_STREAMS
}
fn default_recieve_window_size() -> u64 {
    DEFAULT_MAX_RECIEVE_WINDOW_SIZE
}
fn default_connection_timeout() -> u64 {
    DEFAULT_CONNECTION_TIMEOUT
}
fn default_max_number_of_connections() -> u64 {
    DEFAULT_MAX_NB_CONNECTIONS
}
fn default_max_ack_delay() -> u64 {
    DEFAULT_MAX_ACK_DELAY
}
fn default_ack_exponent() -> u64 {
    DEFAULT_ACK_EXPONENT
}
fn enable_pacing() -> bool {
    DEFAULT_ENABLE_PACING
}
fn default_cc_algorithm() -> String {
    DEFAULT_CC_ALGORITHM.to_string()
}
fn default_incremental_priority() -> bool {
    DEFAULT_INCREMENTAL_PRIORITY
}
fn default_enable_gso() -> bool {
    DEFAULT_ENABLE_GSO
}
fn default_discover_pmtu() -> bool {
    DEFAULT_DISCOVER_PMTU
}
fn default_disconnect_laggy_client() -> bool {
    DEFAULT_DISCONNECT_LAGGY_CLIENTS
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
            enable_pacing: DEFAULT_ENABLE_PACING,
            cc_algorithm: DEFAULT_CC_ALGORITHM.to_string(),
            incremental_priority: DEFAULT_INCREMENTAL_PRIORITY,
            enable_gso: DEFAULT_ENABLE_GSO,
            discover_pmtu: DEFAULT_DISCOVER_PMTU,
            disconnect_laggy_client: DEFAULT_DISCONNECT_LAGGY_CLIENTS,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompressionParameters {
    pub compression_type: CompressionType,
}

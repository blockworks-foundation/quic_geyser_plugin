use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use serde::{Deserialize, Serialize};

use crate::{compression::CompressionType, quic::configure_client::DEFAULT_MAX_STREAMS};

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

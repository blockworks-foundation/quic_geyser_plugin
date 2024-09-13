use std::{fs::read_to_string, path::Path};

use quic_geyser_common::config::ConfigQuicPlugin;
use quic_geyser_snapshot::snapshot_config::SnapshotConfig;
use serde::{Deserialize, Serialize};
use solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub libpath: String,

    pub quic_plugin: ConfigQuicPlugin,

    pub rpc_server: RpcServiceConfig,
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
pub struct RpcServiceConfig {
    #[serde(default = "RpcServiceConfig::default_rpc_service_enable")]
    pub enable: bool,
    #[serde(default = "RpcServiceConfig::default_port")]
    pub port: u16,
    #[serde(default)]
    pub snapshot_config: SnapshotConfig,
}

impl RpcServiceConfig {
    pub fn default_rpc_service_enable() -> bool {
        false
    }

    pub fn default_port() -> u16 {
        10801
    }
}

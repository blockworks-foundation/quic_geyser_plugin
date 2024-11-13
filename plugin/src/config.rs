use std::{fs::read_to_string, path::Path};

use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError;
use quic_geyser_common::config::ConfigQuicPlugin;
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
pub struct RpcServiceConfig {
    #[serde(default = "RpcServiceConfig::default_rpc_service_enable")]
    pub enable: bool,
    #[serde(default = "RpcServiceConfig::default_port")]
    pub port: u16,
}

impl RpcServiceConfig {
    pub fn default_rpc_service_enable() -> bool {
        false
    }
    pub fn default_port() -> u16 {
        10801
    }
}

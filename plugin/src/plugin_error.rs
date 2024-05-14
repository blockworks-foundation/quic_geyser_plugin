use thiserror::Error;

#[derive(Error, Debug)]
pub enum QuicGeyserError {
    ErrorLoadingConfigFile,
    ErrorConfiguringServer,
    MessageChannelClosed,
    UnsupportedVersion,
}

impl std::fmt::Display for QuicGeyserError {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

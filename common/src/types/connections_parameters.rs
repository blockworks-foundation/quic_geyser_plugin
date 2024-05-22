use serde::{Deserialize, Serialize};

use crate::quic::configure_client::{DEFAULT_MAX_RECIEVE_WINDOW_SIZE, DEFAULT_MAX_STREAMS};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct ConnectionParameters {
    pub max_number_of_streams: u64,
    pub recieve_window_size: u64,
    pub timeout_in_seconds: u64,
}

impl Default for ConnectionParameters {
    fn default() -> Self {
        Self {
            max_number_of_streams: DEFAULT_MAX_STREAMS,
            recieve_window_size: DEFAULT_MAX_RECIEVE_WINDOW_SIZE,
            timeout_in_seconds: 10,
        }
    }
}

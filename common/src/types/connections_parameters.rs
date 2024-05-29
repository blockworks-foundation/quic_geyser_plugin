use serde::{Deserialize, Serialize};

use crate::defaults::{
    DEFAULT_ACK_EXPONENT, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_MAX_ACK_DELAY,
    DEFAULT_MAX_RECIEVE_WINDOW_SIZE, DEFAULT_MAX_STREAMS,
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
pub struct ConnectionParameters {
    pub max_number_of_streams: u64,
    pub recieve_window_size: u64,
    pub timeout_in_seconds: u64,
    pub max_ack_delay: u64,
    pub ack_exponent: u64,
}

impl Default for ConnectionParameters {
    fn default() -> Self {
        Self {
            max_number_of_streams: DEFAULT_MAX_STREAMS,
            recieve_window_size: DEFAULT_MAX_RECIEVE_WINDOW_SIZE,
            timeout_in_seconds: DEFAULT_CONNECTION_TIMEOUT,
            max_ack_delay: DEFAULT_MAX_ACK_DELAY,
            ack_exponent: DEFAULT_ACK_EXPONENT,
        }
    }
}

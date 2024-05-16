use serde::{Deserialize, Serialize};

use crate::quic::configure_client::{
    DEFAULT_MAX_SLOT_BLOCKMETA_STREAMS, DEFAULT_MAX_STREAMS, DEFAULT_MAX_TRANSACTION_STREAMS,
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ConnectionParameters {
    pub max_number_of_streams: u32,
    pub streams_for_slot_data: u32,
    pub streams_for_transactions: u32,
}

impl Default for ConnectionParameters {
    fn default() -> Self {
        Self {
            max_number_of_streams: DEFAULT_MAX_STREAMS,
            streams_for_slot_data: DEFAULT_MAX_SLOT_BLOCKMETA_STREAMS,
            streams_for_transactions: DEFAULT_MAX_TRANSACTION_STREAMS,
        }
    }
}

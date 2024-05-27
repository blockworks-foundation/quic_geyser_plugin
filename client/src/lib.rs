pub mod blocking;
pub mod non_blocking;

pub const DEFAULT_MAX_STREAM: u64 = quic_geyser_common::quic::configure_client::DEFAULT_MAX_STREAMS;

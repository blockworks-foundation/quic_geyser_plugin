use boring::ssl::SslMethod;

use crate::quic::configure_server::ALPN_GEYSER_PROTOCOL_ID;

use super::configure_server::MAX_DATAGRAM_SIZE;

pub const DEFAULT_MAX_STREAMS: u32 = 4096;
pub const DEFAULT_MAX_SLOT_BLOCKMETA_STREAMS: u32 = 4;
pub const DEFAULT_MAX_TRANSACTION_STREAMS: u32 = 32;
pub const DEFAULT_MAX_ACCOUNT_STREAMS: u32 =
    DEFAULT_MAX_STREAMS - DEFAULT_MAX_SLOT_BLOCKMETA_STREAMS - DEFAULT_MAX_TRANSACTION_STREAMS;

pub fn configure_client(maximum_concurrent_streams: u32) -> anyhow::Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();

    config.set_max_idle_timeout(5000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(10_000_000);
    config.set_initial_max_stream_data_bidi_local(10_000_000);
    config.set_initial_max_stream_data_bidi_remote(10_000_000);
    config.set_initial_max_stream_data_uni(10_000_000);
    config.set_initial_max_streams_bidi(maximum_concurrent_streams as u64);
    config.set_initial_max_streams_uni(maximum_concurrent_streams as u64);
    config.set_disable_active_migration(true);
    Ok(config)
}

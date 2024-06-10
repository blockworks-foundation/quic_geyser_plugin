use quic_geyser_common::defaults::{ALPN_GEYSER_PROTOCOL_ID, MAX_DATAGRAM_SIZE};

pub fn configure_client(
    maximum_concurrent_streams: u64,
    recieve_window_size: u64,
    timeout_in_seconds: u64,
    maximum_ack_delay: u64,
    ack_exponent: u64,
) -> anyhow::Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    config
        .set_application_protos(&[ALPN_GEYSER_PROTOCOL_ID])
        .unwrap();

    config.set_max_idle_timeout(timeout_in_seconds * 1000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(recieve_window_size);
    config.set_initial_max_stream_data_bidi_local(recieve_window_size);
    config.set_initial_max_stream_data_bidi_remote(recieve_window_size);
    config.set_initial_max_stream_data_uni(recieve_window_size);
    config.set_initial_max_streams_bidi(maximum_concurrent_streams);
    config.set_initial_max_streams_uni(maximum_concurrent_streams);
    config.set_disable_active_migration(true);
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
    config.set_max_ack_delay(maximum_ack_delay);
    config.set_ack_delay_exponent(ack_exponent);
    config.enable_pacing(false);
    Ok(config)
}

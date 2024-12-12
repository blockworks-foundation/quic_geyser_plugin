use quic_geyser_common::{
    defaults::{ALPN_GEYSER_PROTOCOL_ID, MAX_DATAGRAM_SIZE},
    types::connections_parameters::ConnectionParameters,
};

pub fn configure_client(
    connection_parameters: &ConnectionParameters,
) -> anyhow::Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION).unwrap();
    config
        .set_application_protos(&[ALPN_GEYSER_PROTOCOL_ID])
        .unwrap();

    config.set_max_idle_timeout(connection_parameters.timeout_in_seconds * 1000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(connection_parameters.recieve_window_size);
    config.set_initial_max_stream_data_bidi_local(connection_parameters.recieve_window_size);
    config.set_initial_max_stream_data_bidi_remote(connection_parameters.recieve_window_size);
    config.set_initial_max_stream_data_uni(connection_parameters.recieve_window_size);
    config.set_initial_max_streams_bidi(connection_parameters.max_number_of_streams);
    config.set_initial_max_streams_uni(connection_parameters.max_number_of_streams);

    config.set_disable_active_migration(true);
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
    config.set_max_connection_window(48 * 1024 * 1024);
    config.set_max_stream_window(16 * 1024 * 1024);

    config.enable_early_data();
    config.grease(true);
    config.enable_hystart(true);
    config.discover_pmtu(true);

    config.set_active_connection_id_limit(16);
    config.set_max_ack_delay(connection_parameters.max_ack_delay);
    config.set_ack_delay_exponent(connection_parameters.ack_exponent);
    config.set_initial_congestion_window_packets(1024);

    config.enable_pacing(true);
    Ok(config)
}

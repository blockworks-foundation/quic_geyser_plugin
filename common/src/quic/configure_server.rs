use crate::config::QuicParameters;

pub const ALPN_GEYSER_PROTOCOL_ID: &[u8] = b"geyser";
pub const MAX_DATAGRAM_SIZE: usize = 65527; // MAX: 65527

pub fn configure_server(quic_parameter: QuicParameters) -> anyhow::Result<quiche::Config> {
    let max_concurrent_streams = quic_parameter.max_number_of_streams_per_client;
    let recieve_window_size = quic_parameter.recieve_window_size;
    let connection_timeout = quic_parameter.connection_timeout;
    let max_number_of_connections = quic_parameter.max_number_of_connections;
    let maximum_ack_delay = quic_parameter.max_ack_delay;
    let ack_exponent = quic_parameter.ack_exponent;

    let mut config =
        quiche::Config::new(quiche::PROTOCOL_VERSION)
            .expect("Should create config struct");

    config
        .set_application_protos(&[ALPN_GEYSER_PROTOCOL_ID])
        .unwrap();

    config.set_max_idle_timeout(connection_timeout * 1000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(recieve_window_size);
    config.set_initial_max_stream_data_bidi_local(2048);
    config.set_initial_max_stream_data_bidi_remote(2048);
    config.set_initial_max_stream_data_uni(recieve_window_size);
    config.set_initial_max_streams_bidi(max_concurrent_streams);
    config.set_initial_max_streams_uni(max_concurrent_streams);
    config.set_disable_active_migration(true);
    config.set_max_connection_window(128 * 1024 * 1024); // 128 Mbs
    config.enable_early_data();
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::BBR2);
    config.set_active_connection_id_limit(max_number_of_connections);
    config.set_max_ack_delay(maximum_ack_delay);
    config.set_ack_delay_exponent(ack_exponent);
    Ok(config)
}

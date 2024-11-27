use boring::ssl::SslMethod;

use quic_geyser_common::{
    config::QuicParameters,
    defaults::{ALPN_GEYSER_PROTOCOL_ID, MAX_DATAGRAM_SIZE},
};

pub fn configure_server(quic_parameter: &QuicParameters) -> anyhow::Result<quiche::Config> {
    let max_concurrent_streams = quic_parameter.max_number_of_streams_per_client;
    let recieve_window_size = quic_parameter.recieve_window_size;
    let connection_timeout = quic_parameter.connection_timeout;
    let max_number_of_connections = quic_parameter.max_number_of_connections;
    let maximum_ack_delay = quic_parameter.max_ack_delay;
    let ack_exponent = quic_parameter.ack_exponent;
    let enable_pacing = quic_parameter.enable_pacing;
    let cc_algo = quic_parameter.cc_algorithm.as_str();

    let cert = rcgen::generate_simple_self_signed(vec!["quic_geyser".into()]).unwrap();

    let mut boring_ssl_context = boring::ssl::SslContextBuilder::new(SslMethod::tls())?;
    let x509 = boring::x509::X509::from_der(&cert.serialize_der()?)?;

    let private_key_der = cert.serialize_private_key_der();
    let pkey = boring::pkey::PKey::private_key_from_der(&private_key_der)?;
    boring_ssl_context.set_certificate(&x509)?;
    boring_ssl_context.set_private_key(&pkey)?;

    let mut config =
        quiche::Config::with_boring_ssl_ctx_builder(quiche::PROTOCOL_VERSION, boring_ssl_context)
            .expect("Should create config struct");

    config
        .set_application_protos(&[ALPN_GEYSER_PROTOCOL_ID])
        .unwrap();

    config.set_max_idle_timeout(connection_timeout * 1000);
    config.set_max_recv_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_max_send_udp_payload_size(MAX_DATAGRAM_SIZE);
    config.set_initial_max_data(recieve_window_size);
    config.set_initial_max_stream_data_bidi_local(recieve_window_size);
    config.set_initial_max_stream_data_bidi_remote(recieve_window_size);
    config.set_initial_max_stream_data_uni(recieve_window_size);
    config.set_initial_max_streams_bidi(max_concurrent_streams);
    config.set_initial_max_streams_uni(max_concurrent_streams);
    config.set_max_connection_window(48 * 1024 * 1024);
    config.set_max_stream_window(16 * 1024 * 1024);

    config.enable_early_data();
    config.grease(true);
    config.enable_hystart(true);
    config.discover_pmtu(quic_parameter.discover_pmtu);

    if cc_algo == "bbr2" {
        config.set_cc_algorithm(quiche::CongestionControlAlgorithm::BBR2);
    } else if cc_algo == "bbr" {
        config.set_cc_algorithm(quiche::CongestionControlAlgorithm::BBR);
    } else {
        config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
    }

    config.set_active_connection_id_limit(max_number_of_connections);
    config.set_max_ack_delay(maximum_ack_delay);
    config.set_ack_delay_exponent(ack_exponent);
    config.set_initial_congestion_window_packets(1024);
    config.enable_pacing(enable_pacing);
    Ok(config)
}

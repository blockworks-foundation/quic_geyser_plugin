use boring::ssl::SslMethod;

pub const ALPN_GEYSER_PROTOCOL_ID: &[u8] = b"geyser";
pub const MAX_DATAGRAM_SIZE: usize = 1350;

pub fn configure_server(
    max_concurrent_streams: u32,
    recieve_window_size: u64,
    connection_timeout: u64,
) -> anyhow::Result<quiche::Config> {
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
    config.set_initial_max_streams_bidi(max_concurrent_streams as u64);
    config.set_initial_max_streams_uni(max_concurrent_streams as u64);
    config.set_disable_active_migration(true);
    config.enable_early_data();
    Ok(config)
}

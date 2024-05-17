use std::{sync::Arc, time::Duration};

use quinn::{IdleTimeout, ServerConfig};

use super::skip_verification::ServerSkipClientVerification;

pub const ALPN_GEYSER_PROTOCOL_ID: &[u8] = b"quic_geyser_plugin";

pub fn configure_server(
    max_concurrent_streams: u32,
    recieve_window_size: u32,
    connection_timeout: u64,
) -> anyhow::Result<ServerConfig> {
    let cert = rcgen::generate_simple_self_signed(vec!["quic_geyser_server".into()]).unwrap();
    let key = rustls::PrivateKey(cert.serialize_private_key_der());
    let cert = rustls::Certificate(cert.serialize_der().unwrap());

    let mut server_tls_config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_client_cert_verifier(ServerSkipClientVerification::new())
        .with_single_cert(vec![cert], key)?;
    server_tls_config.alpn_protocols = vec![ALPN_GEYSER_PROTOCOL_ID.to_vec()];

    let mut server_config = ServerConfig::with_crypto(Arc::new(server_tls_config));
    server_config.use_retry(true);
    let config = Arc::get_mut(&mut server_config.transport).unwrap();

    config.max_concurrent_uni_streams((max_concurrent_streams).into());
    let recv_size = recieve_window_size.into();
    config.stream_receive_window(recv_size);
    config.receive_window(recv_size);

    let timeout = Duration::from_secs(connection_timeout);
    let timeout = IdleTimeout::try_from(timeout).unwrap();
    config.max_idle_timeout(Some(timeout));

    // disable bidi & datagrams
    config.max_concurrent_bidi_streams(0u32.into());
    config.datagram_receive_buffer_size(None);

    Ok(server_config)
}

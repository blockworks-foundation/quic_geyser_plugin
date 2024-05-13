use std::{net::IpAddr, sync::Arc, time::Duration};

use pem::Pem;
use quinn::{IdleTimeout, ServerConfig};
use solana_sdk::{pubkey::Pubkey, signature::Keypair};
use solana_streamer::{
    quic::QuicServerError,
    tls_certificates::{get_pubkey_from_tls_certificate, new_self_signed_tls_certificate},
};

use super::skip_verification::ServerSkipClientVerification;

pub const ALPN_GEYSER_PROTOCOL_ID: &[u8] = b"quic_geyser_plugin";

pub fn configure_server(
    identity_keypair: &Keypair,
    host: IpAddr,
    max_concurrent_streams: u32,
    recieve_window_size: u32,
    connection_timeout: u64,
) -> Result<(ServerConfig, String), QuicServerError> {
    let (cert, priv_key) = new_self_signed_tls_certificate(identity_keypair, host)?;
    let cert_chain_pem_parts = vec![Pem::new("CERTIFICATE", cert.0.clone())];

    let cert_chain_pem = pem::encode_many(&cert_chain_pem_parts);

    let mut server_tls_config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_client_cert_verifier(ServerSkipClientVerification::new())
        .with_single_cert(vec![cert], priv_key)?;
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

    Ok((server_config, cert_chain_pem))
}

pub fn get_remote_pubkey(connection: &quinn::Connection) -> Option<Pubkey> {
    // Use the client cert only if it is self signed and the chain length is 1.
    connection
        .peer_identity()?
        .downcast::<Vec<rustls::Certificate>>()
        .ok()
        .filter(|certs| certs.len() == 1)?
        .first()
        .and_then(get_pubkey_from_tls_certificate)
}

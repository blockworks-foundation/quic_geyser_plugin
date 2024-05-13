use std::{
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
    time::Duration,
};

use quinn::{
    ClientConfig, Endpoint, EndpointConfig, IdleTimeout, TokioRuntime, TransportConfig, VarInt,
};
use solana_sdk::signature::Keypair;
use solana_streamer::tls_certificates::new_self_signed_tls_certificate;

use crate::quic::{
    configure_server::ALPN_GEYSER_PROTOCOL_ID, skip_verification::ClientSkipServerVerification,
};

pub fn create_client_endpoint(
    certificate: rustls::Certificate,
    key: rustls::PrivateKey,
    maximum_streams: u32,
) -> Endpoint {
    const DATAGRAM_RECEIVE_BUFFER_SIZE: usize = 64 * 1024 * 1024;
    const DATAGRAM_SEND_BUFFER_SIZE: usize = 64 * 1024 * 1024;
    const INITIAL_MAXIMUM_TRANSMISSION_UNIT: u16 = MINIMUM_MAXIMUM_TRANSMISSION_UNIT;
    const MINIMUM_MAXIMUM_TRANSMISSION_UNIT: u16 = 2000;

    let mut endpoint = {
        let client_socket =
            solana_net_utils::bind_in_range(IpAddr::V4(Ipv4Addr::UNSPECIFIED), (8000, 10000))
                .expect("create_endpoint bind_in_range")
                .1;
        let mut config = EndpointConfig::default();
        config
            .max_udp_payload_size(MINIMUM_MAXIMUM_TRANSMISSION_UNIT)
            .expect("Should set max MTU");
        quinn::Endpoint::new(config, None, client_socket, Arc::new(TokioRuntime))
            .expect("create_endpoint quinn::Endpoint::new")
    };

    let mut crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(ClientSkipServerVerification {}))
        .with_client_auth_cert(vec![certificate], key)
        .unwrap();
    crypto.enable_early_data = true;
    crypto.alpn_protocols = vec![ALPN_GEYSER_PROTOCOL_ID.to_vec()];

    let mut config = ClientConfig::new(Arc::new(crypto));
    let mut transport_config = TransportConfig::default();

    let timeout = IdleTimeout::try_from(Duration::from_secs(600)).unwrap();
    transport_config.max_idle_timeout(Some(timeout));
    transport_config.keep_alive_interval(Some(Duration::from_secs(1)));
    transport_config.datagram_receive_buffer_size(Some(DATAGRAM_RECEIVE_BUFFER_SIZE));
    transport_config.datagram_send_buffer_size(DATAGRAM_SEND_BUFFER_SIZE);
    transport_config.initial_mtu(INITIAL_MAXIMUM_TRANSMISSION_UNIT);
    transport_config.max_concurrent_bidi_streams(VarInt::from_u32(0));
    transport_config.max_concurrent_uni_streams(VarInt::from(maximum_streams));
    transport_config.min_mtu(MINIMUM_MAXIMUM_TRANSMISSION_UNIT);
    transport_config.mtu_discovery_config(None);
    transport_config.enable_segmentation_offload(false);
    config.transport_config(Arc::new(transport_config));

    endpoint.set_default_client_config(config);

    endpoint
}

pub async fn configure_client(
    identity: &Keypair,
    maximum_concurrent_streams: u32,
) -> anyhow::Result<Endpoint> {
    let (certificate, key) =
        new_self_signed_tls_certificate(identity, IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))?;
    Ok(create_client_endpoint(
        certificate,
        key,
        maximum_concurrent_streams,
    ))
}

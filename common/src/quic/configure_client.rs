use std::{net::UdpSocket, sync::Arc, time::Duration};

use quinn::{
    ClientConfig, Endpoint, EndpointConfig, IdleTimeout, TokioRuntime, TransportConfig, VarInt,
};

use crate::quic::{
    configure_server::ALPN_GEYSER_PROTOCOL_ID, skip_verification::ClientSkipServerVerification,
};

pub const DEFAULT_MAX_STREAMS: u32 = 4096;
pub const DEFAULT_MAX_SLOT_BLOCKMETA_STREAMS: u32 = 4;
pub const DEFAULT_MAX_TRANSACTION_STREAMS: u32 = 32;
pub const DEFAULT_MAX_ACCOUNT_STREAMS: u32 =
    DEFAULT_MAX_STREAMS - DEFAULT_MAX_SLOT_BLOCKMETA_STREAMS - DEFAULT_MAX_TRANSACTION_STREAMS;

pub fn create_client_endpoint(maximum_streams: u32) -> Endpoint {
    const DATAGRAM_RECEIVE_BUFFER_SIZE: usize = 64 * 1024 * 1024;
    const DATAGRAM_SEND_BUFFER_SIZE: usize = 64 * 1024 * 1024;
    const INITIAL_MAXIMUM_TRANSMISSION_UNIT: u16 = MINIMUM_MAXIMUM_TRANSMISSION_UNIT;
    const MINIMUM_MAXIMUM_TRANSMISSION_UNIT: u16 = 2000;

    let mut endpoint = {
        let client_socket = UdpSocket::bind("0.0.0.0:0").expect("Client socket should be binded");
        let mut config = EndpointConfig::default();
        config
            .max_udp_payload_size(MINIMUM_MAXIMUM_TRANSMISSION_UNIT)
            .expect("Should set max MTU");
        quinn::Endpoint::new(config, None, client_socket, Arc::new(TokioRuntime))
            .expect("create_endpoint quinn::Endpoint::new")
    };

    let cert = rcgen::generate_simple_self_signed(vec!["quic_geyser_client".into()]).unwrap();
    let key = rustls::PrivateKey(cert.serialize_private_key_der());
    let cert = rustls::Certificate(cert.serialize_der().unwrap());

    let mut crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(Arc::new(ClientSkipServerVerification {}))
        .with_client_auth_cert(vec![cert], key)
        .expect("Should create client config");

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

pub async fn configure_client(maximum_concurrent_streams: u32) -> anyhow::Result<Endpoint> {
    Ok(create_client_endpoint(maximum_concurrent_streams))
}

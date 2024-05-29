use quic_geyser_common::defaults::ALPN_GEYSER_PROTOCOL_ID;
use quic_geyser_common::defaults::DEFAULT_MAX_RECIEVE_WINDOW_SIZE;
use quic_geyser_common::defaults::MAX_DATAGRAM_SIZE;
use quic_geyser_common::filters::Filter;
use quic_geyser_common::message::Message;
use quic_geyser_common::types::connections_parameters::ConnectionParameters;
use quinn::{
    ClientConfig, Connection, ConnectionError, Endpoint, EndpointConfig, IdleTimeout, RecvStream,
    SendStream, TokioRuntime, TransportConfig, VarInt,
};
use std::net::{SocketAddr, UdpSocket};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

pub fn create_client_endpoint(connection_parameters: ConnectionParameters) -> Endpoint {
    const MINIMUM_MAXIMUM_TRANSMISSION_UNIT: u16 = 2000;
    const INITIAL_MAXIMUM_TRANSMISSION_UNIT: u16 = MINIMUM_MAXIMUM_TRANSMISSION_UNIT;

    let mut endpoint = {
        let client_socket = UdpSocket::bind("0.0.0.0:0").expect("Client socket should be binded");
        let mut config = EndpointConfig::default();
        config
            .max_udp_payload_size(MAX_DATAGRAM_SIZE as u16)
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

    let timeout = IdleTimeout::try_from(Duration::from_secs(
        connection_parameters.timeout_in_seconds,
    ))
    .unwrap();
    transport_config.max_idle_timeout(Some(timeout));
    transport_config.keep_alive_interval(Some(Duration::from_secs(1)));
    transport_config
        .datagram_receive_buffer_size(Some(connection_parameters.recieve_window_size as usize));
    transport_config.datagram_send_buffer_size(connection_parameters.recieve_window_size as usize);
    transport_config.initial_mtu(INITIAL_MAXIMUM_TRANSMISSION_UNIT);
    transport_config.max_concurrent_bidi_streams(VarInt::from(
        connection_parameters.max_number_of_streams as u32,
    ));
    transport_config.max_concurrent_uni_streams(VarInt::from(
        connection_parameters.max_number_of_streams as u32,
    ));
    transport_config.min_mtu(MINIMUM_MAXIMUM_TRANSMISSION_UNIT);
    transport_config.mtu_discovery_config(None);
    transport_config.enable_segmentation_offload(false);
    config.transport_config(Arc::new(transport_config));

    endpoint.set_default_client_config(config);

    endpoint
}

pub async fn recv_message(
    mut recv_stream: RecvStream,
    timeout_in_seconds: u64,
) -> anyhow::Result<Message> {
    let mut buffer: Vec<u8> = vec![];

    while let Some(data) = tokio::time::timeout(
        Duration::from_secs(timeout_in_seconds),
        recv_stream.read_chunk(DEFAULT_MAX_RECIEVE_WINDOW_SIZE as usize, true),
    )
    .await??
    {
        let bytes = data.bytes.to_vec();
        buffer.extend_from_slice(&bytes);
    }
    Ok(bincode::deserialize::<Message>(&buffer)?)
}

pub struct Client {
    connection: Connection,
}

pub async fn send_message(mut send_stream: SendStream, message: &Message) -> anyhow::Result<()> {
    let binary = bincode::serialize(&message)?;
    send_stream.write_all(&binary).await?;
    send_stream.finish().await?;
    Ok(())
}

impl Client {
    pub async fn new(
        server_address: String,
        connection_parameters: ConnectionParameters,
    ) -> anyhow::Result<(Client, tokio::sync::mpsc::UnboundedReceiver<Message>)> {
        let timeout: u64 = connection_parameters.timeout_in_seconds;
        let endpoint = create_client_endpoint(connection_parameters);
        let socket_addr = SocketAddr::from_str(&server_address)?;
        let connecting = endpoint.connect(socket_addr, "quic_geyser_client")?;

        let (message_sx_queue, message_rx_queue) =
            tokio::sync::mpsc::unbounded_channel::<Message>();

        let connection = connecting.await?;
        {
            let connection = connection.clone();
            tokio::spawn(async move {
                loop {
                    let stream = connection.accept_uni().await;
                    match stream {
                        Ok(recv_stream) => {
                            let sender = message_sx_queue.clone();
                            tokio::spawn(async move {
                                let message = recv_message(recv_stream, timeout).await;
                                match message {
                                    Ok(message) => {
                                        let _ = sender.send(message);
                                    }
                                    Err(e) => {
                                        log::trace!("Error getting message {}", e);
                                    }
                                }
                            });
                        }
                        Err(e) => match &e {
                            ConnectionError::ConnectionClosed(_)
                            | ConnectionError::ApplicationClosed(_)
                            | ConnectionError::LocallyClosed => {
                                break;
                            }
                            _ => {
                                log::error!("Got {} while listing to the connection", e);
                            }
                        },
                    }
                }
            });
        }

        Ok((Client { connection }, message_rx_queue))
    }

    pub async fn subscribe(&self, filters: Vec<Filter>) -> anyhow::Result<()> {
        let send_stream = self.connection.open_uni().await?;
        send_message(send_stream, &Message::Filters(filters)).await?;
        Ok(())
    }
}

pub struct ClientSkipServerVerification;

impl ClientSkipServerVerification {
    pub fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for ClientSkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use quic_geyser_common::{
        channel_message::AccountData,
        compression::CompressionType,
        config::{CompressionParameters, ConfigQuicPlugin, QuicParameters},
        filters::Filter,
        message::Message,
        types::{
            account::Account, connections_parameters::ConnectionParameters,
            slot_identifier::SlotIdentifier,
        },
    };
    use quic_geyser_server::quic_server::QuicServer;
    use solana_sdk::pubkey::Pubkey;
    use std::{net::SocketAddr, thread::sleep, time::Duration};

    pub fn get_account_for_test(slot: u64, data_size: usize) -> Account {
        Account {
            slot_identifier: SlotIdentifier { slot },
            pubkey: Pubkey::new_unique(),
            owner: Pubkey::new_unique(),
            write_version: 0,
            lamports: 12345,
            rent_epoch: u64::MAX,
            executable: false,
            data: (0..data_size).map(|_| rand::random::<u8>()).collect_vec(),
            compression_type: CompressionType::None,
            data_length: data_size as u64,
        }
    }

    use crate::non_blocking::client::Client;

    #[tokio::test]
    pub async fn test_non_blocking_client() {
        let server_sock: SocketAddr = "0.0.0.0:20000".parse().unwrap();
        let url = format!("127.0.0.1:{}", server_sock.port());

        let msg_acc_1 = Message::AccountMsg(get_account_for_test(0, 2));
        let msg_acc_2 = Message::AccountMsg(get_account_for_test(1, 20));
        let msg_acc_3 = Message::AccountMsg(get_account_for_test(2, 100));
        let msg_acc_4 = Message::AccountMsg(get_account_for_test(3, 1_000));
        let msg_acc_5 = Message::AccountMsg(get_account_for_test(4, 10_000));
        let msgs = [msg_acc_1, msg_acc_2, msg_acc_3, msg_acc_4, msg_acc_5];

        let jh = {
            let msgs = msgs.clone();
            let server_sock = server_sock.clone();
            std::thread::spawn(move || {
                let config = ConfigQuicPlugin {
                    address: server_sock,
                    quic_parameters: QuicParameters::default(),
                    compression_parameters: CompressionParameters {
                        compression_type: CompressionType::None,
                    },
                    number_of_retries: 100,
                    log_level: "debug".to_string(),
                    allow_accounts: true,
                    allow_accounts_at_startup: false,
                };
                let quic_server = QuicServer::new(config).unwrap();
                // wait for client to connect and subscribe
                sleep(Duration::from_secs(2));
                for msg in msgs {
                    let Message::AccountMsg(account) = msg else {
                        panic!("should never happen");
                    };
                    quic_server
                        .send_message(
                            quic_geyser_common::channel_message::ChannelMessage::Account(
                                AccountData {
                                    pubkey: account.pubkey,
                                    account: account.solana_account(),
                                    write_version: account.write_version,
                                },
                                account.slot_identifier.slot,
                            ),
                        )
                        .unwrap();
                }
                sleep(Duration::from_secs(1));
            })
        };
        // wait for server to start
        sleep(Duration::from_secs(1));

        // server started
        let (client, mut reciever) = Client::new(
            url,
            ConnectionParameters {
                max_number_of_streams: 10,
                recieve_window_size: 1_000_000,
                timeout_in_seconds: 10,
                max_ack_delay: 25,
                ack_exponent: 3,
            },
        )
        .await
        .unwrap();
        client.subscribe(vec![Filter::AccountsAll]).await.unwrap();

        let mut cnt = 0;
        for message_sent in msgs {
            let msg = reciever.recv().await.unwrap();
            log::info!("got message : {}", cnt);
            cnt += 1;
            assert_eq!(message_sent, msg);
        }
        jh.join().unwrap();
    }
}

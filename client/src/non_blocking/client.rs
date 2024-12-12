use anyhow::bail;
use quic_geyser_common::defaults::ALPN_GEYSER_PROTOCOL_ID;
use quic_geyser_common::defaults::DEFAULT_MAX_RECIEVE_WINDOW_SIZE;
use quic_geyser_common::defaults::MAX_PAYLOAD_BUFFER;
use quic_geyser_common::filters::Filter;
use quic_geyser_common::message::Message;
use quic_geyser_common::net::parse_host_port;
use quic_geyser_common::types::connections_parameters::ConnectionParameters;
use quinn::{
    ClientConfig, ConnectionError, Endpoint, EndpointConfig, IdleTimeout, RecvStream, SendStream,
    TokioRuntime, TransportConfig, VarInt,
};
use std::net::UdpSocket;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;

pub fn create_client_endpoint(connection_parameters: ConnectionParameters) -> Endpoint {
    let mut endpoint = {
        let client_socket = UdpSocket::bind(parse_host_port("[::]:0").unwrap())
            .expect("Client socket should be binded");
        let mut config = EndpointConfig::default();
        config
            .max_udp_payload_size(MAX_PAYLOAD_BUFFER.try_into().unwrap())
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
    transport_config.max_concurrent_bidi_streams(VarInt::from(0_u32));
    transport_config.max_concurrent_uni_streams(VarInt::from(
        connection_parameters.max_number_of_streams as u32,
    ));
    transport_config.enable_segmentation_offload(connection_parameters.enable_gso);

    transport_config.crypto_buffer_size(64 * 1024);
    transport_config
        .receive_window(VarInt::from_u64(connection_parameters.recieve_window_size).unwrap());
    transport_config.stream_receive_window(VarInt::from_u64(10 * 1024 * 1024).unwrap());

    config.transport_config(Arc::new(transport_config));

    endpoint.set_default_client_config(config);

    endpoint
}

// pub async fn recv_message(
//     mut recv_stream: RecvStream,
//     timeout_in_seconds: u64,
// ) -> anyhow::Result<Message> {
//     let mut buffer = Vec::<u8>::new();
//     buffer.reserve(128 * 1024); // reserve 128 kbs for each message

//     while let Some(data) = tokio::time::timeout(
//         Duration::from_secs(timeout_in_seconds),
//         recv_stream.read_chunk(DEFAULT_MAX_RECIEVE_WINDOW_SIZE as usize, true),
//     )
//     .await??
//     {
//         buffer.extend_from_slice(&data.bytes);
//     }
//     Ok(bincode::deserialize::<Message>(&buffer)?)
// }

pub struct Client {
    filter_sender: tokio::sync::mpsc::UnboundedSender<Vec<Filter>>,
}

pub async fn send_message(send_stream: &mut SendStream, message: &Message) -> anyhow::Result<()> {
    let binary = message.to_binary_stream();
    send_stream.write_all(&binary).await?;
    send_stream.flush().await?;
    Ok(())
}

impl Client {
    pub async fn new(
        server_address: String,
        connection_parameters: ConnectionParameters,
    ) -> anyhow::Result<(
        Client,
        tokio::sync::mpsc::UnboundedReceiver<Message>,
        Vec<tokio::task::JoinHandle<anyhow::Result<()>>>,
    )> {
        let endpoint = create_client_endpoint(connection_parameters);
        let socket_addr = parse_host_port(&server_address)?;
        let connecting = endpoint.connect(socket_addr, "quic_geyser_client")?;

        let (message_sx_queue, message_rx_queue) =
            tokio::sync::mpsc::unbounded_channel::<Message>();

        let connection = connecting.await?;
        let jh1 = {
            let connection = connection.clone();
            tokio::spawn(async move {
                loop {
                    // sender is closed / no messages to send
                    if message_sx_queue.is_closed() {
                        bail!("quic client stopped, sender closed");
                    }
                    let stream: Result<RecvStream, ConnectionError> = connection.accept_uni().await;
                    match stream {
                        Ok(mut recv_stream) => {
                            let message_sx_queue = message_sx_queue.clone();
                            tokio::spawn(async move {
                                let mut buffer: Vec<u8> = vec![];
                                'read_loop: loop {
                                    match recv_stream
                                        .read_chunk(DEFAULT_MAX_RECIEVE_WINDOW_SIZE as usize, true)
                                        .await
                                    {
                                        Ok(Some(chunk)) => {
                                            buffer.extend_from_slice(&chunk.bytes);
                                            while let Some((message, size)) =
                                                Message::from_binary_stream(&buffer)
                                            {
                                                if let Err(e) = message_sx_queue.send(message) {
                                                    log::error!("Message sent error : {:?}", e);
                                                    break 'read_loop;
                                                }
                                                buffer.drain(..size);
                                            }
                                        }
                                        Ok(None) => {
                                            log::warn!("Chunk none");
                                        }
                                        Err(e) => {
                                            log::debug!("Error getting message {:?}", e);
                                            break;
                                        }
                                    }
                                }
                            });
                        }
                        Err(e) => match &e {
                            ConnectionError::ConnectionClosed(_)
                            | ConnectionError::ApplicationClosed(_)
                            | ConnectionError::LocallyClosed => {
                                log::debug!("Got {:?} while listing to the connection", e);
                                break;
                            }
                            _ => {
                                log::error!("Got {:?} while listing to the connection", e);
                                break;
                            }
                        },
                    }
                }
                bail!("quic client stopped, connection lost")
            })
        };

        // create a ping thread and subscribe thread
        let (filter_sender, mut filter_rx) = tokio::sync::mpsc::unbounded_channel();
        let jh2 = {
            let connection = connection.clone();
            tokio::spawn(async move {
                let mut uni_stream = connection.open_uni().await?;

                loop {
                    tokio::select! {
                        filters = filter_rx.recv() => {
                            if let Some(filters) = filters {
                                log::debug!("Sending server filters: {filters:?} on {}", uni_stream.id());
                                if let Err(e) = send_message(&mut uni_stream, &Message::Filters(filters)).await {
                                    log::error!("Error while sending filters : {e:?}");
                                }
                            }
                            break;
                        },
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {
                            send_message( &mut uni_stream, &Message::Ping).await?;
                        }
                    }
                }
                // keep sending pings
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if let Err(e) = send_message(&mut uni_stream, &Message::Ping).await {
                        log::error!("Error while sending ping message : {e:?}");
                        break;
                    }
                }
                Ok(())
            })
        };

        Ok((Client { filter_sender }, message_rx_queue, vec![jh1, jh2]))
    }

    pub async fn subscribe(&self, filters: Vec<Filter>) -> anyhow::Result<()> {
        self.filter_sender.send(filters)?;
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
        net::parse_host_port,
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
        tracing_subscriber::fmt::init();
        let server_sock: SocketAddr = parse_host_port("0.0.0.0:20000").unwrap();
        let url = format!("127.0.0.1:{}", server_sock.port());

        let msg_acc_1 = Message::AccountMsg(get_account_for_test(0, 2));
        let msg_acc_2 = Message::AccountMsg(get_account_for_test(1, 20));
        let msg_acc_3 = Message::AccountMsg(get_account_for_test(2, 100));
        let msg_acc_4 = Message::AccountMsg(get_account_for_test(3, 1_000));
        let msg_acc_5 = Message::AccountMsg(get_account_for_test(4, 10_000));
        let msg_acc_6 = Message::AccountMsg(get_account_for_test(4, 10_000_000));
        let msgs = [
            msg_acc_1, msg_acc_2, msg_acc_3, msg_acc_4, msg_acc_5, msg_acc_6,
        ];

        let jh = {
            let msgs = msgs.clone();
            std::thread::spawn(move || {
                let config = ConfigQuicPlugin {
                    address: server_sock,
                    quic_parameters: QuicParameters {
                        discover_pmtu: false,
                        ..Default::default()
                    },
                    compression_parameters: CompressionParameters {
                        compression_type: CompressionType::None,
                    },
                    number_of_retries: 100,
                    log_level: "debug".to_string(),
                    allow_accounts: true,
                    allow_accounts_at_startup: false,
                    enable_block_builder: false,
                    build_blocks_with_accounts: false,
                };
                let quic_server = QuicServer::new(config).unwrap();
                // wait for client to connect and subscribe
                sleep(Duration::from_secs(2));
                for msg in msgs {
                    log::info!("sending message");
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
                                false,
                            ),
                        )
                        .unwrap();
                }
                sleep(Duration::from_secs(1));
            })
        };
        // wait for server to start
        sleep(Duration::from_millis(10));

        // server started
        let (client, mut reciever, _tasks) = Client::new(
            url,
            ConnectionParameters {
                max_number_of_streams: 10,
                recieve_window_size: 1_000_000,
                timeout_in_seconds: 10,
                max_ack_delay: 25,
                ack_exponent: 3,
                enable_gso: false,
                enable_pacing: false,
            },
        )
        .await
        .unwrap();

        log::info!("subscribing");
        client.subscribe(vec![Filter::AccountsAll]).await.unwrap();
        log::info!("subscribed");
        sleep(Duration::from_millis(100));

        for (cnt, message_sent) in msgs.iter().enumerate() {
            let msg = reciever.recv().await.unwrap();
            log::info!("got message : {}", cnt);
            assert_eq!(*message_sent, msg);
        }
        jh.join().unwrap();
    }
}

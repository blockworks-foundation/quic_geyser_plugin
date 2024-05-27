use quic_geyser_common::filters::Filter;
use quic_geyser_common::message::Message;
use quic_geyser_common::quic::configure_client::configure_client;
use quic_geyser_common::quic::quiche_client_loop::client_loop;
use quic_geyser_common::types::connections_parameters::ConnectionParameters;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

pub struct Client {
    is_connected: Arc<AtomicBool>,
    filters_sender: std::sync::mpsc::Sender<Message>,
}

impl Client {
    pub fn new(
        server_address: String,
        connection_parameters: ConnectionParameters,
    ) -> anyhow::Result<(Client, tokio::sync::mpsc::UnboundedReceiver<Message>)> {
        let config = configure_client(
            connection_parameters.max_number_of_streams,
            connection_parameters.recieve_window_size,
            connection_parameters.timeout_in_seconds,
            connection_parameters.max_ack_delay,
            connection_parameters.ack_exponent,
        )?;
        let server_address: SocketAddr = server_address.parse()?;
        let socket_addr: SocketAddr = "0.0.0.0:0"
            .parse()
            .expect("Socket address should be returned");
        let is_connected = Arc::new(AtomicBool::new(false));
        let (filters_sender, rx_sent_queue) = std::sync::mpsc::channel();
        let (sx_recv_queue, client_rx_queue) = std::sync::mpsc::channel();

        let is_connected_client = is_connected.clone();
        let _client_loop_jh = std::thread::spawn(move || {
            if let Err(e) = client_loop(
                config,
                socket_addr,
                server_address,
                rx_sent_queue,
                sx_recv_queue,
                is_connected_client.clone(),
            ) {
                log::error!("client stopped with error {e}");
            }
            is_connected_client.store(false, std::sync::atomic::Ordering::Relaxed);
        });

        let (tokio_sx_queue, tokio_rx_queue) = tokio::sync::mpsc::unbounded_channel::<Message>();
        let _tokio_depile_loop = std::thread::spawn(move || {
            while let Ok(message) = client_rx_queue.recv() {
                if tokio_sx_queue.send(message).is_err() {
                    break;
                }
            }
        });

        Ok((
            Client {
                is_connected,
                filters_sender,
            },
            tokio_rx_queue,
        ))
    }

    pub fn subscribe(&self, filters: Vec<Filter>) -> anyhow::Result<()> {
        let message = Message::Filters(filters);
        self.filters_sender.send(message)?;
        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.is_connected.load(std::sync::atomic::Ordering::Relaxed)
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
        quic::quic_server::QuicServer,
        types::{
            account::Account, connections_parameters::ConnectionParameters,
            slot_identifier::SlotIdentifier,
        },
    };
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
        .unwrap();
        client.subscribe(vec![Filter::AccountsAll]).unwrap();

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

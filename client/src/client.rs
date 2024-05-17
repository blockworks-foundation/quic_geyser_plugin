use std::{net::SocketAddr, str::FromStr};

use async_stream::stream;
use futures::Stream;
use quic_geyser_common::message::Message;
use quic_geyser_common::quic::configure_client::configure_client;
use quic_geyser_common::quic::quinn_reciever::recv_message;
use quic_geyser_common::quic::quinn_sender::send_message;
use quic_geyser_common::{filters::Filter, types::connections_parameters::ConnectionParameters};
use quinn::{Connection, ConnectionError};
use solana_sdk::signature::Keypair;

pub struct Client {
    pub address: String,
    connection: Connection,
}

impl Client {
    pub async fn new(
        server_address: String,
        identity: &Keypair,
        connection_parameters: ConnectionParameters,
    ) -> anyhow::Result<Client> {
        let endpoint =
            configure_client(identity, connection_parameters.max_number_of_streams).await?;
        let socket_addr = SocketAddr::from_str(&server_address)?;
        let connecting = endpoint.connect(socket_addr, "quic_geyser_client")?;
        let connection = connecting.await?;
        let send_stream = connection.open_uni().await?;
        send_message(
            send_stream,
            Message::ConnectionParameters(connection_parameters),
        )
        .await?;

        Ok(Client {
            address: server_address,
            connection,
        })
    }

    pub async fn subscribe(&self, filters: Vec<Filter>) -> anyhow::Result<()> {
        let send_stream = self.connection.open_uni().await?;
        send_message(send_stream, Message::Filters(filters)).await?;
        Ok(())
    }

    pub fn get_stream(&self) -> impl Stream<Item = Message> {
        let connection = self.connection.clone();
        let (sender, mut reciever) = tokio::sync::mpsc::unbounded_channel::<Message>();
        tokio::spawn(async move {
            loop {
                let stream = connection.accept_uni().await;
                match stream {
                    Ok(recv_stream) => {
                        let sender = sender.clone();
                        tokio::spawn(async move {
                            let message = recv_message(recv_stream, 10).await;
                            match message {
                                Ok(message) => {
                                    let _ = sender.send(message);
                                }
                                Err(e) => {
                                    log::error!("Error getting message {}", e);
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
        stream! {
            while let Some(message) = reciever.recv().await {
                    yield message;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, UdpSocket},
        sync::Arc,
    };

    use futures::StreamExt;
    use quic_geyser_common::{
        filters::{AccountFilter, Filter},
        message::Message,
        quic::{configure_server::configure_server, connection_manager::ConnectionManager},
        types::{account::Account, connections_parameters::ConnectionParameters},
    };
    use quinn::{Endpoint, EndpointConfig, TokioRuntime};
    use solana_sdk::{pubkey::Pubkey, signature::Keypair};
    use tokio::{pin, sync::Notify};

    use crate::client::Client;

    #[tokio::test]
    pub async fn test_client() {
        let (config, _) = configure_server(
            &Keypair::new(),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            1,
            100000,
            1,
        )
        .unwrap();

        let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
        let port = sock.local_addr().unwrap().port();
        let url = format!("127.0.0.1:{}", port);
        let notify_server_start = Arc::new(Notify::new());
        let notify_subscription = Arc::new(Notify::new());

        let msg_acc_1 = Message::AccountMsg(Account::get_account_for_test(0, 2));
        let msg_acc_2 = Message::AccountMsg(Account::get_account_for_test(1, 20));
        let msg_acc_3 = Message::AccountMsg(Account::get_account_for_test(2, 100));
        let msg_acc_4 = Message::AccountMsg(Account::get_account_for_test(3, 1000));
        let msg_acc_5 = Message::AccountMsg(Account::get_account_for_test(4, 10000));
        let msgs = [msg_acc_1, msg_acc_2, msg_acc_3, msg_acc_4, msg_acc_5];

        {
            let msgs = msgs.clone();
            let notify_server_start = notify_server_start.clone();
            let notify_subscription = notify_subscription.clone();
            tokio::spawn(async move {
                let endpoint = Endpoint::new(
                    EndpointConfig::default(),
                    Some(config),
                    sock,
                    Arc::new(TokioRuntime),
                )
                .unwrap();

                let (connection_manager, _jh) = ConnectionManager::new(endpoint);
                notify_server_start.notify_one();
                notify_subscription.notified().await;
                for msg in msgs {
                    connection_manager.dispatch(msg, 10).await;
                }
            });
        }

        notify_server_start.notified().await;
        // server started

        let client = Client::new(
            url,
            &Keypair::new(),
            ConnectionParameters {
                max_number_of_streams: 3,
                streams_for_slot_data: 1,
                streams_for_transactions: 1,
            },
        )
        .await
        .unwrap();
        client
            .subscribe(vec![Filter::Account(AccountFilter {
                owner: Some(Pubkey::default()),
                accounts: None,
            })])
            .await
            .unwrap();

        notify_subscription.notify_one();

        let stream = client.get_stream();
        pin!(stream);
        for _ in 0..5 {
            let msg = stream.next().await.unwrap();
            match &msg {
                Message::AccountMsg(account) => {
                    let index = account.slot_identifier.slot as usize;
                    let sent_message = &msgs[index];
                    assert_eq!(*sent_message, msg);
                }
                _ => {
                    panic!("should only get account messages")
                }
            }
        }
    }
}

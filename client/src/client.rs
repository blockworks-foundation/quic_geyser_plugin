use std::{net::SocketAddr, str::FromStr};

use async_stream::stream;
use futures::Stream;
use quic_geyser_common::filters::Filter;
use quic_geyser_common::message::Message;
use quic_geyser_common::quic::configure_client::configure_client;
use quic_geyser_common::quic::quinn_reciever::recv_message;
use quic_geyser_common::quic::quinn_sender::send_message;
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
        max_concurrent_streams: u32,
    ) -> anyhow::Result<Client> {
        let endpoint = configure_client(identity, max_concurrent_streams).await?;
        let socket_addr = SocketAddr::from_str(&server_address)?;
        let connecting = endpoint.connect(socket_addr, "quic_geyser_client")?;
        let connection = connecting.await?;

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
                            let message = recv_message(recv_stream).await;
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

use quinn::{Connection, Endpoint};
use std::sync::Arc;
use std::{collections::VecDeque, time::Duration};
use tokio::{sync::RwLock, task::JoinHandle, time::Instant};

use crate::{filters::Filter, message::Message};

use super::{quinn_reciever::recv_message, quinn_sender::send_message};

pub struct ConnectionData {
    pub id: u64,
    pub connection: Connection,
    pub filters: Vec<Filter>,
    pub since: Instant,
}

impl ConnectionData {
    pub fn new(id: u64, connection: Connection) -> Self {
        Self {
            id,
            connection,
            filters: vec![],
            since: Instant::now(),
        }
    }
}

/*
    This class will take care of adding connections and filters etc
*/
pub struct ConnectionManager {
    connections: Arc<RwLock<VecDeque<ConnectionData>>>,
}

impl ConnectionManager {
    pub fn new(endpoint: Endpoint) -> (Self, JoinHandle<()>) {
        let connections: Arc<RwLock<VecDeque<ConnectionData>>> =
            Arc::new(RwLock::new(VecDeque::new()));
        // create a task to add incoming connections
        let connection_adder_jh = {
            let connections = connections.clone();
            tokio::spawn(async move {
                let mut id = 0;
                loop {
                    // acceept incoming connections
                    if let Some(connecting) = endpoint.accept().await {
                        let connection_result = connecting.await;
                        match connection_result {
                            Ok(connection) => {
                                // connection established
                                // add the connection in the connections list
                                let mut lk = connections.write().await;
                                id += 1;
                                let current_id = id;
                                lk.push_back(ConnectionData::new(current_id, connection.clone()));
                                drop(lk);

                                let connections_tmp = connections.clone();

                                // task to add filters
                                let connection_to_listen = connection.clone();
                                tokio::spawn(async move {
                                    loop {
                                        if let Ok(recv_stream) =
                                            connection_to_listen.accept_uni().await
                                        {
                                            match tokio::time::timeout(
                                                Duration::from_secs(10),
                                                recv_message(recv_stream),
                                            )
                                            .await
                                            {
                                                Ok(Ok(filters)) => {
                                                    let Message::Filters(mut filters) = filters
                                                    else {
                                                        continue;
                                                    };
                                                    let mut lk = connections_tmp.write().await;
                                                    let connection_data =
                                                        lk.iter_mut().find(|x| x.id == current_id);
                                                    if let Some(connection_data) = connection_data {
                                                        connection_data
                                                            .filters
                                                            .append(&mut filters);
                                                    }
                                                }
                                                Ok(Err(e)) => {
                                                    log::error!("error getting message from the client : {}", e);
                                                }
                                                Err(_timeout) => {
                                                    log::warn!("Client request timeout");
                                                }
                                            }
                                        }
                                    }
                                });

                                let connections = connections.clone();
                                // create a connection removing task
                                tokio::spawn(async move {
                                    // if connection is closed remove it
                                    let closed_error = connection.closed().await;
                                    log::info!("connection closed with error {}", closed_error);
                                    let mut lk = connections.write().await;
                                    lk.retain(|x| x.id != current_id);
                                });
                            }
                            Err(e) => log::error!("Error connecting {}", e),
                        }
                    }
                }
            })
        };

        (Self { connections }, connection_adder_jh)
    }

    pub async fn dispach(&self, message: Message, retry_count: u64) {
        let lk = self.connections.read().await;

        for connection_data in lk.iter() {
            if connection_data.filters.iter().any(|x| x.allows(&message)) {
                let connection = connection_data.connection.clone();
                let message = message.clone();
                tokio::spawn(async move {
                    for _ in 0..retry_count {
                        let send_stream = connection.open_uni().await;
                        match send_stream {
                            Ok(send_stream) => {
                                match send_message(send_stream, message.clone()).await {
                                    Ok(_) => {
                                        log::debug!("Message sucessfully sent");
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "error dispatching message and sending data : {}",
                                            e
                                        )
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!(
                                    "error dispatching message while creating stream : {}",
                                    e
                                );
                            }
                        }
                    }
                });
            }
        }
    }
}

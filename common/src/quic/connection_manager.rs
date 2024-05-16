use quinn::{Connection, Endpoint};
use std::sync::Arc;
use std::{collections::VecDeque, time::Duration};
use tokio::sync::Semaphore;
use tokio::{sync::RwLock, task::JoinHandle, time::Instant};

use crate::types::connections_parameters::ConnectionParameters;
use crate::{filters::Filter, message::Message};

use super::{quinn_reciever::recv_message, quinn_sender::send_message};

#[derive(Debug)]
pub struct ConnectionData {
    pub id: u64,
    pub connection: Connection,
    pub filters: Vec<Filter>,
    pub since: Instant,
    stream_semaphore_for_accounts: Arc<Semaphore>,
    stream_semaphore_for_slot_data: Arc<Semaphore>,
    stream_semaphore_for_transactions: Arc<Semaphore>,
}

impl ConnectionData {
    pub fn new(
        id: u64,
        connection: Connection,
        connections_parameters: ConnectionParameters,
    ) -> Self {
        let accounts_streams_count = connections_parameters
            .max_number_of_streams
            .saturating_sub(connections_parameters.streams_for_slot_data)
            .saturating_sub(connections_parameters.streams_for_transactions);
        Self {
            id,
            connection,
            filters: vec![],
            since: Instant::now(),
            stream_semaphore_for_accounts: Arc::new(Semaphore::new(
                accounts_streams_count as usize,
            )),
            stream_semaphore_for_slot_data: Arc::new(Semaphore::new(
                connections_parameters.streams_for_slot_data as usize,
            )),
            stream_semaphore_for_transactions: Arc::new(Semaphore::new(
                connections_parameters.streams_for_transactions as usize,
            )),
        }
    }
}

/*
    This class will take care of adding connections and filters etc
*/
#[derive(Debug, Clone)]
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
                let mut id: u64 = 0;
                loop {
                    // accept incoming connections
                    if let Some(connecting) = endpoint.accept().await {
                        let connections = connections.clone();
                        id += 1;
                        let current_id = id;
                        tokio::spawn(async move {
                            let connection_result =
                                tokio::time::timeout(Duration::from_secs(10), connecting).await;
                            match connection_result {
                                Ok(Ok(connection)) => {
                                    let parameter_stream = tokio::time::timeout(
                                        Duration::from_secs(10),
                                        connection.accept_uni(),
                                    )
                                    .await;
                                    match parameter_stream {
                                        Ok(Ok(recv_stream)) => {
                                            let message = recv_message(recv_stream, 10).await;
                                            if let Ok(Message::ConnectionParameters(
                                                connections_parameters,
                                            )) = message
                                            {
                                                Self::manage_new_connection(
                                                    connections,
                                                    connection,
                                                    current_id,
                                                    connections_parameters,
                                                )
                                                .await;
                                            }
                                        }
                                        Ok(Err(e)) => {
                                            log::error!(
                                                "connection params unistream errored {}",
                                                e
                                            );
                                        }
                                        Err(_timeout) => {
                                            log::error!("connection params unistream timeout")
                                        }
                                    }
                                }
                                Ok(Err(e)) => log::error!("Error connecting {}", e),
                                Err(_elapsed) => log::error!("Connection timeout"),
                            }
                        });
                    }
                }
            })
        };

        (Self { connections }, connection_adder_jh)
    }

    async fn manage_new_connection(
        connections: Arc<RwLock<VecDeque<ConnectionData>>>,
        connection: Connection,
        current_id: u64,
        connections_parameters: ConnectionParameters,
    ) {
        // connection established
        // add the connection in the connections list
        let mut lk = connections.write().await;
        log::info!("New connection id: {}", current_id);
        lk.push_back(ConnectionData::new(
            current_id,
            connection.clone(),
            connections_parameters,
        ));
        drop(lk);

        let connections_tmp = connections.clone();

        // task to add filters
        let connection_to_listen = connection.clone();
        tokio::spawn(async move {
            loop {
                if let Ok(recv_stream) = connection_to_listen.accept_uni().await {
                    log::info!("new unistream connection to update filters");
                    match tokio::time::timeout(
                        Duration::from_secs(10),
                        recv_message(recv_stream, 10),
                    )
                    .await
                    {
                        Ok(Ok(filters)) => {
                            let Message::Filters(mut filters) = filters else {
                                continue;
                            };
                            let mut lk = connections_tmp.write().await;
                            let connection_data = lk.iter_mut().find(|x| x.id == current_id);
                            if let Some(connection_data) = connection_data {
                                connection_data.filters.append(&mut filters);
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
            log::info!(
                "connection closed with id {} error {}",
                closed_error,
                current_id
            );
            let mut lk = connections.write().await;
            lk.retain(|x| x.id != current_id);
        });
    }

    pub async fn dispach(&self, message: Message, retry_count: u64) {
        let lk = self.connections.read().await;

        for connection_data in lk.iter() {
            if connection_data.filters.iter().any(|x| x.allows(&message)) {
                let connection = connection_data.connection.clone();

                let message = message.clone();
                let (message_type, semaphore) = match &message {
                    Message::SlotMsg(_) => (
                        "slot",
                        connection_data.stream_semaphore_for_slot_data.clone(),
                    ),
                    Message::BlockMetaMsg(_) => (
                        "block meta",
                        connection_data.stream_semaphore_for_slot_data.clone(),
                    ),
                    Message::TransactionMsg(_) => (
                        "transactions",
                        connection_data.stream_semaphore_for_transactions.clone(),
                    ),
                    _ => (
                        "accounts",
                        connection_data.stream_semaphore_for_accounts.clone(),
                    ),
                };
                let id = connection_data.id;

                tokio::spawn(async move {
                    let permit_result = semaphore.clone().try_acquire_owned();

                    let _permit = match permit_result {
                        Ok(permit) => permit,
                        Err(_) => {
                            // all permits are taken wait log warning and wait for permit
                            log::warn!(
                                "Stream {} seems to be lagging for {} message type",
                                id,
                                message_type
                            );
                            semaphore
                                .acquire_owned()
                                .await
                                .expect("Should aquire the permit")
                        }
                    };

                    for _ in 0..retry_count {
                        let send_stream = connection.open_uni().await;
                        match send_stream {
                            Ok(send_stream) => {
                                match send_message(send_stream, message.clone()).await {
                                    Ok(_) => {
                                        log::debug!("Message sucessfully sent");
                                        break;
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
                                break;
                            }
                        }
                    }
                });
            }
        }
    }
}

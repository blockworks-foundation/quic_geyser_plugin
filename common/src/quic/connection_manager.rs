use quinn::{Connection, Endpoint};
use std::sync::Arc;
use std::{collections::VecDeque, time::Duration};
use tokio::sync::Semaphore;
use tokio::{sync::RwLock, task::JoinHandle, time::Instant};

use crate::{filters::Filter, message::Message};

use super::{quinn_reciever::recv_message, quinn_sender::send_message};

#[derive(Debug)]
pub struct ConnectionData {
    pub id: u64,
    pub connection: Connection,
    pub filters: Vec<Filter>,
    pub since: Instant,
    streams_under_use: Arc<Semaphore>,
}

impl ConnectionData {
    pub fn new(id: u64, connection: Connection, max_streams_count: usize) -> Self {
        Self {
            id,
            connection,
            filters: vec![],
            since: Instant::now(),
            streams_under_use: Arc::new(Semaphore::new(max_streams_count)),
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
    pub fn new(endpoint: Endpoint, max_streams_count: usize) -> (Self, JoinHandle<()>) {
        let connections: Arc<RwLock<VecDeque<ConnectionData>>> =
            Arc::new(RwLock::new(VecDeque::new()));
        // create a task to add incoming connections
        let connection_adder_jh = {
            let connections = connections.clone();
            tokio::spawn(async move {
                let mut id = 0;
                loop {
                    // accept incoming connections
                    if let Some(connecting) = endpoint.accept().await {
                        let connection_result = connecting.await;
                        match connection_result {
                            Ok(connection) => {
                                println!(
                                    "Connection started ---------------------------------------"
                                );
                                // connection established
                                // add the connection in the connections list
                                let mut lk = connections.write().await;
                                id += 1;
                                let current_id = id;
                                log::info!("New connection id: {}", current_id);
                                lk.push_back(ConnectionData::new(
                                    current_id,
                                    connection.clone(),
                                    max_streams_count,
                                ));
                                drop(lk);

                                let connections_tmp = connections.clone();

                                // task to add filters
                                let connection_to_listen = connection.clone();
                                tokio::spawn(async move {
                                    loop {
                                        if let Ok(recv_stream) =
                                            connection_to_listen.accept_uni().await
                                        {
                                            log::info!(
                                                "new unistream connection to update filters"
                                            );
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
                                    log::info!(
                                        "connection closed with id {} error {}",
                                        closed_error,
                                        current_id
                                    );
                                    let mut lk = connections.write().await;
                                    lk.retain(|x| x.id != current_id);
                                    println!(
                                        "Connection closed ---------------------------------------"
                                    );
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
                let stream_under_use = connection_data.streams_under_use.clone();
                let id = connection_data.id;

                tokio::spawn(async move {
                    let permit_result = stream_under_use.clone().try_acquire_owned();

                    let _permit = match permit_result {
                        Ok(permit) => permit,
                        Err(_) => {
                            // all permits are taken wait log warning and wait for permit
                            log::warn!("Stream {} seems to be lagging", id);
                            stream_under_use
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

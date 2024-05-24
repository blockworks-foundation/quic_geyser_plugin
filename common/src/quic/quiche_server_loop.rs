use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{mpsc, Arc, Mutex, RwLock},
    thread::{self, sleep, JoinHandle},
    time::Duration,
};

use anyhow::bail;
use itertools::Itertools;
use quiche::{Connection, ConnectionId};
use ring::rand::SystemRandom;

use crate::{
    channel_message::ChannelMessage,
    compression::CompressionType,
    filters::Filter,
    message::Message,
    quic::{
        quiche_reciever::recv_message,
        quiche_sender::{handle_writable, send_message},
        quiche_utils::{get_next_unidi, mint_token, validate_token},
    },
    types::{account::Account, block_meta::SlotMeta, slot_identifier::SlotIdentifier},
};

use super::{
    configure_server::MAX_DATAGRAM_SIZE, quiche_reciever::ReadStreams,
    quiche_utils::PartialResponses,
};

struct Client {
    pub connection: Connection,
    pub partial_responses: PartialResponses,
    pub read_streams: ReadStreams,
    pub next_stream: u64,
}

type ClientMap = RwLock<HashMap<quiche::ConnectionId<'static>, Mutex<Client>>>;
type FilterMap = RwLock<HashMap<quiche::ConnectionId<'static>, Vec<Filter>>>;

pub fn server_loop(
    mut config: quiche::Config,
    socket_addr: SocketAddr,
    message_send_queue: mpsc::Receiver<ChannelMessage>,
    compression_type: CompressionType,
    stop_laggy_client: bool,
) -> anyhow::Result<()> {
    let maximum_concurrent_streams = u64::MAX;
    let socket = std::net::UdpSocket::bind(socket_addr)?;
    socket.set_nonblocking(true)?;

    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    let local_addr = socket.local_addr()?;
    let rng = SystemRandom::new();
    let conn_id_seed = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap();
    let clients = Arc::new(ClientMap::new(HashMap::new()));
    let filter_map = Arc::new(FilterMap::new(HashMap::new()));

    create_thread_to_dispatch_messages(
        clients.clone(),
        filter_map.clone(),
        message_send_queue,
        maximum_concurrent_streams,
        compression_type,
        stop_laggy_client,
    );

    // thread to close connections
    {
        let clients = clients.clone();
        let filter_map = filter_map.clone();

        std::thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(5));
            let mut clients = clients.write().unwrap();
            clients.retain(|id, client| {
                let mut client = client.lock().unwrap();
                client.connection.on_timeout();
                if client.connection.is_closed() {
                    filter_map.write().unwrap().remove(id);
                    log::info!(
                        "{} connection closed {:?}",
                        client.connection.trace_id(),
                        client.connection.stats()
                    );
                }

                !client.connection.is_closed()
            });
        });
    }

    let network_read = true;
    let mut last_write: Option<(Vec<u8>, quiche::SendInfo)> = None;
    loop {
        if network_read {
            'read: loop {
                let (len, from) = match socket.recv_from(&mut buf) {
                    Ok(v) => v,
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            log::trace!("recv() would block");
                            break 'read;
                        }
                        bail!("recv() failed: {:?}", e);
                    }
                };

                let pkt_buf = &mut buf[..len];

                // Parse the QUIC packet's header.
                let hdr = match quiche::Header::from_slice(pkt_buf, quiche::MAX_CONN_ID_LEN) {
                    Ok(v) => v,

                    Err(e) => {
                        log::error!("Parsing packet header failed: {:?}", e);
                        continue 'read;
                    }
                };

                let conn_id = ring::hmac::sign(&conn_id_seed, &hdr.dcid);
                let conn_id = &conn_id.as_ref()[..quiche::MAX_CONN_ID_LEN];
                let conn_id: ConnectionId<'static> = conn_id.to_vec().into();
                let clients_rl = clients.read().unwrap();
                if !clients_rl.contains_key(&hdr.dcid) && !clients_rl.contains_key(&conn_id) {
                    drop(clients_rl);
                    if hdr.ty != quiche::Type::Initial {
                        log::error!("Packet is not Initial");
                        continue 'read;
                    }

                    if !quiche::version_is_supported(hdr.version) {
                        log::warn!("Doing version negotiation");
                        let len =
                            quiche::negotiate_version(&hdr.scid, &hdr.dcid, &mut out).unwrap();

                        let out = &out[..len];

                        if let Err(e) = socket.send_to(out, from) {
                            if e.kind() == std::io::ErrorKind::WouldBlock {
                                break;
                            }
                            panic!("send() failed: {:?}", e);
                        }
                        continue 'read;
                    }

                    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
                    scid.copy_from_slice(&conn_id);

                    let scid = quiche::ConnectionId::from_ref(&scid);

                    // Token is always present in Initial packets.
                    let token = hdr.token.as_ref().unwrap();

                    // Do stateless retry if the client didn't send a token.
                    if token.is_empty() {
                        log::debug!("Doing stateless retry");

                        let new_token = mint_token(&hdr, &from);

                        let len = quiche::retry(
                            &hdr.scid,
                            &hdr.dcid,
                            &scid,
                            &new_token,
                            hdr.version,
                            &mut out,
                        )
                        .unwrap();

                        let out = &out[..len];

                        if let Err(e) = socket.send_to(out, from) {
                            if e.kind() == std::io::ErrorKind::WouldBlock {
                                break;
                            }

                            panic!("send() failed: {:?}", e);
                        }
                        continue 'read;
                    }

                    let odcid = validate_token(&from, token);

                    if odcid.is_none() {
                        log::error!("Invalid address validation token");
                        continue 'read;
                    }

                    if scid.len() != hdr.dcid.len() {
                        log::error!("Invalid destination connection ID");
                        continue 'read;
                    }

                    let scid = hdr.dcid.clone();

                    log::info!("New connection: dcid={:?} scid={:?}", hdr.dcid, scid);

                    let mut conn =
                        quiche::accept(&scid, odcid.as_ref(), local_addr, from, &mut config)?;

                    let recv_info = quiche::RecvInfo {
                        to: socket.local_addr().unwrap(),
                        from,
                    };
                    // Process potentially coalesced packets.
                    match conn.recv(pkt_buf, recv_info) {
                        Ok(v) => v,
                        Err(e) => {
                            log::error!("{} recv failed: {:?}", conn.trace_id(), e);
                            continue 'read;
                        }
                    };

                    let client = Client {
                        connection: conn,
                        partial_responses: PartialResponses::new(),
                        read_streams: ReadStreams::new(),
                        next_stream: get_next_unidi(0, true, maximum_concurrent_streams),
                    };

                    let mut client_wl = clients.write().unwrap();
                    client_wl.insert(scid.clone(), Mutex::new(client));
                } else {
                    // get the existing client
                    let client = match clients_rl.get(&hdr.dcid) {
                        Some(v) => v,
                        None => clients_rl
                            .get(&conn_id)
                            .expect("The client should exist in the map"),
                    };

                    let recv_info = quiche::RecvInfo {
                        to: socket.local_addr().unwrap(),
                        from,
                    };
                    let mut client = client.lock().unwrap();
                    // Process potentially coalesced packets.
                    match client.connection.recv(pkt_buf, recv_info) {
                        Ok(v) => v,
                        Err(e) => {
                            log::error!("{} recv failed: {:?}", client.connection.trace_id(), e);
                            continue 'read;
                        }
                    };
                };
            }
        }

        // Generate outgoing QUIC packets for all active connections and send
        // them on the UDP socket, until quiche reports that there are no more
        // packets to be sent.
        let clients_rl = clients.read().unwrap();

        for (id, c) in clients_rl.iter() {
            let lk = c.lock();
            match lk {
                Ok(mut client) => {
                    let Client {
                        connection: conn,
                        read_streams,
                        partial_responses,
                        ..
                    } = &mut *client;

                    if conn.is_in_early_data() || conn.is_established() {
                        // Process all readable streams.
                        for stream in conn.readable() {
                            let message = recv_message(conn, read_streams, stream);
                            match message {
                                Ok(Some(message)) => match message {
                                    Message::Filters(mut filters) => {
                                        let mut filters_lk = filter_map.write().unwrap();
                                        match filters_lk.get_mut(id) {
                                            Some(f) => {
                                                f.append(&mut filters);
                                            }
                                            None => {
                                                filters_lk.insert(id.clone(), filters);
                                            }
                                        }
                                    }
                                    Message::Ping => {
                                        // got ping
                                    }
                                    _ => {
                                        log::error!("unknown message from the client");
                                    }
                                },
                                Ok(None) => {}
                                Err(e) => {
                                    log::error!("Error recieving message : {e}")
                                }
                            }
                        }
                    }

                    for stream_id in conn.writable() {
                        handle_writable(conn, partial_responses, stream_id);
                    }

                    loop {
                        if let Some((buf, send_info)) = &last_write {
                            match socket.send_to(&buf, send_info.to) {
                                Ok(_len) => {
                                    last_write = None;
                                }
                                Err(e) => {
                                    if e.kind() == std::io::ErrorKind::WouldBlock {
                                        log::warn!("writing would block");
                                        break;
                                    }
                                    log::error!("send() failed: {:?}", e);
                                }
                            }
                        }

                        let (write, send_info) = match conn.send(&mut out) {
                            Ok(v) => v,
                            Err(quiche::Error::Done) => {
                                log::trace!("{} done writing", conn.trace_id());
                                break;
                            }
                            Err(e) => {
                                log::error!(
                                    "{} send failed: {:?}, closing connection",
                                    conn.trace_id(),
                                    e
                                );
                                conn.close(false, 0x1, b"fail").ok();
                                break;
                            }
                        };

                        match socket.send_to(&out[..write], send_info.to) {
                            Ok(_len) => {
                                // log::debug!("wrote: {} bytes", len);
                            }
                            Err(e) => {
                                if e.kind() == std::io::ErrorKind::WouldBlock {
                                    log::warn!("writing would block");
                                    let vec = out[..write].to_vec();
                                    last_write = Some((vec, send_info));
                                    break;
                                }
                                log::error!("send() failed: {:?}", e);
                            }
                        }
                    }
                }
                Err(_) => {
                    log::error!("client lock poisoned");
                }
            }
        }
    }
}

fn create_thread_to_dispatch_messages(
    clients: Arc<ClientMap>,
    filters_maps: Arc<FilterMap>,
    message_send_queue: mpsc::Receiver<ChannelMessage>,
    maximum_concurrent_streams: u64,
    compression_type: CompressionType,
    stop_laggy_client: bool,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        while let Ok(message) = message_send_queue.recv() {
            let dispatch_to = filters_maps
                .read()
                .unwrap()
                .iter()
                .filter_map(|(id, filters)| {
                    if filters.iter().any(|x| x.allows(&message)) {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect_vec();

            if !dispatch_to.is_empty() {
                let (message, priority) = match message {
                    ChannelMessage::Account(account, slot) => {
                        let slot_identifier = SlotIdentifier { slot };
                        let geyser_account = Account::new(
                            account.pubkey,
                            account.account,
                            compression_type,
                            slot_identifier,
                            account.write_version,
                        );

                        (Message::AccountMsg(geyser_account), 4)
                    }
                    ChannelMessage::Slot(slot, parent, commitment_level) => (
                        Message::SlotMsg(SlotMeta {
                            slot,
                            parent,
                            commitment_level,
                        }),
                        1,
                    ),
                    ChannelMessage::BlockMeta(block_meta) => (Message::BlockMetaMsg(block_meta), 2),
                    ChannelMessage::Transaction(transaction) => {
                        (Message::TransactionMsg(transaction), 3)
                    }
                };
                let binary =
                    bincode::serialize(&message).expect("Message should be serializable in binary");
                let clients = clients.read().unwrap();
                for client_id in dispatch_to {
                    let Some(client) = clients.get(&client_id) else {
                        log::error!("client not found");
                        continue;
                    };
                    let mut client = client.lock().unwrap();

                    let Client {
                        connection: conn,
                        partial_responses,
                        next_stream,
                        ..
                    } = &mut *client;

                    if conn.is_closed() || !conn.is_established() {
                        continue;
                    }

                    let stream_id = *next_stream;

                    *next_stream = get_next_unidi(stream_id, true, maximum_concurrent_streams);

                    match conn.stream_priority(stream_id, priority, true) {
                        Ok(_) => {
                            log::trace!("priority was set correctly");
                        }
                        Err(e) => {
                            log::error!(
                                "Unable to set priority for the stream {}, error {}",
                                stream_id,
                                e
                            );
                        }
                    }
                    log::trace!("dispatching {} on stream id : {}", binary.len(), stream_id);
                    if let Err(e) = send_message(conn, partial_responses, stream_id, &binary) {
                        log::error!("Error sending message : {e}");
                        if stop_laggy_client {
                            if let Err(e) = client.connection.close(true, 1, b"laggy client") {
                                if e != quiche::Error::Done {
                                    log::error!("error closing client : {}", e);
                                }
                            } else {
                                log::info!(
                                    "Stopping laggy client : {} because of error : {}",
                                    client.connection.trace_id(),
                                    e
                                );
                            }
                        }
                    }
                }
            }
        }
    })
}

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex, RwLock,
    },
};

use anyhow::bail;
use itertools::Itertools;
use quiche::ConnectionId;
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

struct DispatchingData {
    pub sender: Sender<(Vec<u8>, u8)>,
    pub filters: Arc<RwLock<Vec<Filter>>>,
}

type DispachingConnections = Arc<Mutex<HashMap<ConnectionId<'static>, DispatchingData>>>;

const MAX_MESSAGE_DEPILE_IN_LOOP: usize = 1024;

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
    let mut clients: HashMap<
        quiche::ConnectionId<'static>,
        mpsc::Sender<(quiche::RecvInfo, Vec<u8>)>,
    > = HashMap::new();

    let (write_sender, write_reciver) = mpsc::channel::<(quiche::SendInfo, Vec<u8>)>();

    let mut last_write: Option<(quiche::SendInfo, Vec<u8>)> = None;
    let dispatching_connections: DispachingConnections = Arc::new(Mutex::new(HashMap::<
        ConnectionId<'static>,
        DispatchingData,
    >::new()));

    create_dispatching_thread(
        message_send_queue,
        dispatching_connections.clone(),
        compression_type,
    );

    loop {
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
            if !clients.contains_key(&hdr.dcid) && !clients.contains_key(&conn_id) {
                if hdr.ty != quiche::Type::Initial {
                    log::error!("Packet is not Initial");
                    continue 'read;
                }

                if !quiche::version_is_supported(hdr.version) {
                    log::warn!("Doing version negotiation");
                    let len = quiche::negotiate_version(&hdr.scid, &hdr.dcid, &mut out).unwrap();

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

                let (client_sender, client_reciver) = mpsc::channel();

                let (client_message_sx, client_message_rx) = mpsc::channel();

                let filters = Arc::new(RwLock::new(Vec::new()));
                create_client_task(
                    conn,
                    client_reciver,
                    write_sender.clone(),
                    client_message_rx,
                    filters.clone(),
                    maximum_concurrent_streams,
                    stop_laggy_client,
                );
                let mut lk = dispatching_connections.lock().unwrap();
                lk.insert(
                    scid.clone(),
                    DispatchingData {
                        sender: client_message_sx,
                        filters,
                    },
                );
                clients.insert(scid, client_sender);
            } else {
                // get the existing client
                let client = match clients.get(&hdr.dcid) {
                    Some(v) => v,
                    None => clients
                        .get(&conn_id)
                        .expect("The client should exist in the map"),
                };

                let recv_info = quiche::RecvInfo {
                    to: socket.local_addr().unwrap(),
                    from,
                };
                if client.send((recv_info, pkt_buf.to_vec())).is_err() {
                    // client is closed
                    clients.remove(&hdr.dcid);
                    clients.remove(&conn_id);
                }
            };
        }

        let mut do_write = true;
        if let Some((send_info, buf)) = &last_write {
            match socket.send_to(buf, send_info.to) {
                Ok(_len) => {
                    last_write = None;
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        log::warn!("writing would block");
                        do_write = false;
                    }
                    log::error!("send() failed: {:?}", e);
                }
            }
        }

        if do_write {
            while let Ok((send_info, buffer)) = write_reciver.try_recv() {
                match socket.send_to(&buffer, send_info.to) {
                    Ok(_len) => {
                        // log::debug!("wrote: {} bytes", len);
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            log::warn!("writing would block");
                            last_write = Some((send_info, buffer));
                            break;
                        }
                        log::error!("send() failed: {:?}", e);
                    }
                }
            }
        }
    }
}

fn create_client_task(
    connection: quiche::Connection,
    receiver: mpsc::Receiver<(quiche::RecvInfo, Vec<u8>)>,
    sender: mpsc::Sender<(quiche::SendInfo, Vec<u8>)>,
    message_channel: mpsc::Receiver<(Vec<u8>, u8)>,
    filters: Arc<RwLock<Vec<Filter>>>,
    maximum_concurrent_streams: u64,
    stop_laggy_client: bool,
) {
    std::thread::spawn(move || {
        let mut partial_responses = PartialResponses::new();
        let mut read_streams = ReadStreams::new();
        let mut next_stream: u64 = 1;
        let mut connection = connection;
        loop {
            while let Ok((info, mut buf)) = receiver.try_recv() {
                let buf = buf.as_mut_slice();
                match connection.recv(buf, info) {
                    Ok(v) => v,
                    Err(e) => {
                        log::error!("{} recv failed: {:?}", connection.trace_id(), e);
                        break;
                    }
                };
            }

            if connection.is_in_early_data() || connection.is_established() {
                // Process all readable streams.
                for stream in connection.readable() {
                    let message = recv_message(&mut connection, &mut read_streams, stream);
                    match message {
                        Ok(Some(message)) => match message {
                            Message::Filters(mut f) => {
                                let mut filter_lk = filters.write().unwrap();
                                filter_lk.append(&mut f);
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

            for stream_id in connection.writable() {
                handle_writable(&mut connection, &mut partial_responses, stream_id);
            }

            loop {
                let mut out = [0; MAX_DATAGRAM_SIZE];
                match connection.send(&mut out) {
                    Ok((len, send_info)) => {
                        sender.send((send_info, out[..len].to_vec())).unwrap();
                    }
                    Err(quiche::Error::Done) => {
                        log::trace!("{} done writing", connection.trace_id());
                        break;
                    }
                    Err(e) => {
                        log::error!(
                            "{} send failed: {:?}, closing connection",
                            connection.trace_id(),
                            e
                        );
                        connection.close(false, 0x1, b"fail").ok();
                        break;
                    }
                };
            }

            for _ in 0..MAX_MESSAGE_DEPILE_IN_LOOP {
                if let Ok((message, priority)) = message_channel.try_recv() {
                    if connection.is_closed() || !connection.is_established() {
                        continue;
                    }

                    let stream_id = next_stream;

                    next_stream = get_next_unidi(stream_id, true, maximum_concurrent_streams);

                    match connection.stream_priority(stream_id, priority, true) {
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
                    log::trace!("dispatching {} on stream id : {}", message.len(), stream_id);
                    if let Err(e) =
                        send_message(&mut connection, &mut partial_responses, stream_id, &message)
                    {
                        log::error!("Error sending message : {e}");
                        if stop_laggy_client {
                            if let Err(e) = connection.close(true, 1, b"laggy client") {
                                if e != quiche::Error::Done {
                                    log::error!("error closing client : {}", e);
                                }
                            } else {
                                log::info!(
                                    "Stopping laggy client : {} because of error : {}",
                                    connection.trace_id(),
                                    e
                                );
                            }
                        }
                    }
                }
            }

            connection.on_timeout();
            if connection.is_closed() {
                log::info!(
                    "{} connection closed {:?}",
                    connection.trace_id(),
                    connection.stats()
                );
                break;
            }
        }
    });
}

fn create_dispatching_thread(
    message_send_queue: mpsc::Receiver<ChannelMessage>,
    dispatching_connections: DispachingConnections,
    compression_type: CompressionType,
) {
    std::thread::spawn(move || {
        while let Ok(message) = message_send_queue.recv() {
            let mut dispatching_connections_lk = dispatching_connections.lock().unwrap();

            let dispatching_connections = dispatching_connections_lk
                .iter()
                .filter_map(|(id, x)| {
                    let filters = x.filters.read().unwrap();
                    if filters.iter().any(|x| x.allows(&message)) {
                        Some(id.clone())
                    } else {
                        None
                    }
                })
                .collect_vec();

            if !dispatching_connections.is_empty() {
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
                for id in dispatching_connections.iter() {
                    if dispatching_connections_lk
                        .get(id)
                        .unwrap()
                        .sender
                        .send((binary.clone(), priority))
                        .is_err()
                    {
                        // client is closed
                        dispatching_connections_lk.remove(id);
                    }
                }
            }
        }
    });
}

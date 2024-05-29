use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    sync::{
        atomic::AtomicU64,
        mpsc::{self, Sender},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant},
};

use anyhow::bail;
use itertools::Itertools;
use quiche::ConnectionId;
use ring::rand::SystemRandom;

use quic_geyser_common::{
    channel_message::ChannelMessage,
    compression::CompressionType,
    defaults::{MAX_ALLOWED_PARTIAL_RESPONSES, MAX_DATAGRAM_SIZE},
    filters::Filter,
    message::Message,
    types::{account::Account, block_meta::SlotMeta, slot_identifier::SlotIdentifier},
};

use crate::{
    quiche_reciever::recv_message,
    quiche_sender::{handle_writable, send_message},
    quiche_utils::{get_next_unidi, mint_token, validate_token},
};

use super::{
    quiche_reciever::ReadStreams,
    quiche_utils::{write_to_socket, PartialResponses},
};

struct DispatchingData {
    pub sender: Sender<(Vec<u8>, u8)>,
    pub filters: Arc<RwLock<Vec<Filter>>>,
    pub message_counter: Arc<AtomicU64>,
}

type DispachingConnections = Arc<Mutex<HashMap<ConnectionId<'static>, DispatchingData>>>;

const MAX_MESSAGE_DEPILE_IN_LOOP: usize = 32;
const ACCEPTABLE_PACING_DELAY: Duration = Duration::from_millis(100);

struct Packet {
    pub buffer: Vec<u8>,
    pub to: SocketAddr,
}

pub fn server_loop(
    mut config: quiche::Config,
    socket_addr: SocketAddr,
    message_send_queue: mpsc::Receiver<ChannelMessage>,
    compression_type: CompressionType,
    stop_laggy_client: bool,
    max_number_of_streams: u64,
) -> anyhow::Result<()> {
    let maximum_concurrent_streams_id = u64::MAX;

    let mut socket = mio::net::UdpSocket::bind(socket_addr)?;
    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(1024);

    poll.registry().register(
        &mut socket,
        mio::Token(0),
        mio::Interest::READABLE | mio::Interest::WRITABLE,
    )?;

    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    let local_addr = socket.local_addr()?;
    let rng = SystemRandom::new();
    let conn_id_seed = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap();
    let mut clients: HashMap<
        quiche::ConnectionId<'static>,
        mpsc::Sender<(quiche::RecvInfo, Vec<u8>)>,
    > = HashMap::new();

    let (write_sender, write_reciver) = std::sync::mpsc::channel::<(quiche::SendInfo, Vec<u8>)>();

    // poll.registry().register(
    //     &mut write_reciver,
    //     mio::Token(1),
    //     mio::Interest::READABLE,
    // )?;

    let mut packets_to_send: BTreeMap<Instant, Packet> = BTreeMap::new();

    let dispatching_connections: DispachingConnections = Arc::new(Mutex::new(HashMap::<
        ConnectionId<'static>,
        DispatchingData,
    >::new()));

    create_dispatching_thread(
        message_send_queue,
        dispatching_connections.clone(),
        compression_type,
        max_number_of_streams,
    );

    loop {
        poll.poll(&mut events, Some(Duration::from_micros(100)))?;
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

                    if write_to_socket(&socket, &out[..len], from) {
                        break;
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
                let message_counter = Arc::new(AtomicU64::new(0));

                let filters = Arc::new(RwLock::new(Vec::new()));
                create_client_task(
                    conn,
                    client_reciver,
                    write_sender.clone(),
                    client_message_rx,
                    filters.clone(),
                    maximum_concurrent_streams_id,
                    stop_laggy_client,
                    message_counter.clone(),
                );
                let mut lk = dispatching_connections.lock().unwrap();
                lk.insert(
                    scid.clone(),
                    DispatchingData {
                        sender: client_message_sx,
                        filters,
                        message_counter,
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

        let instant = Instant::now();
        // send remaining packets
        // log::debug!("packets to send : {}", packets_to_send.len());
        while let Some((to_send, packet)) = packets_to_send.first_key_value() {
            if instant + ACCEPTABLE_PACING_DELAY <= *to_send {
                break;
            }
            if write_to_socket(&socket, &packet.buffer, packet.to) {
                break;
            }
            packets_to_send.pop_first();
        }

        let instant = Instant::now();
        while let Ok((send_info, buffer)) = write_reciver.try_recv() {
            if send_info.at > instant + ACCEPTABLE_PACING_DELAY {
                packets_to_send.insert(
                    send_info.at,
                    Packet {
                        buffer,
                        to: send_info.to,
                    },
                );
            } else if write_to_socket(&socket, &buffer, send_info.to) {
                packets_to_send.insert(
                    send_info.at,
                    Packet {
                        buffer,
                        to: send_info.to,
                    },
                );
                break;
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
    maximum_concurrent_streams_id: u64,
    stop_laggy_client: bool,
    message_count: Arc<AtomicU64>,
) {
    std::thread::spawn(move || {
        let mut partial_responses = PartialResponses::new();
        let mut read_streams = ReadStreams::new();
        let mut next_stream: u64 = 3;
        let mut connection = connection;
        let mut instance = Instant::now();
        let mut closed = false;
        let mut out = [0; MAX_DATAGRAM_SIZE];

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
                while handle_writable(&mut connection, &mut partial_responses, stream_id) {}
            }

            for _ in 0..MAX_MESSAGE_DEPILE_IN_LOOP {
                if partial_responses.len() > MAX_ALLOWED_PARTIAL_RESPONSES {
                    // too many streams open already try to fullfill them
                    break;
                }
                if connection.is_closed() || !connection.is_established() {
                    break;
                }
                let close = match message_channel.try_recv() {
                    Ok((message, priority)) => {
                        message_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                        let stream_id = next_stream;

                        next_stream =
                            get_next_unidi(stream_id, true, maximum_concurrent_streams_id);

                        if let Err(e) = connection.stream_priority(stream_id, priority, true) {
                            if !closed {
                                log::error!(
                                    "Unable to set priority for the stream {}, error {}",
                                    stream_id,
                                    e
                                );
                            }
                        }
                        if send_message(
                            &mut connection,
                            &mut partial_responses,
                            stream_id,
                            &message,
                        ).is_err() {
                            true
                        } else {
                            false
                        }
                    }
                    Err(e) => {
                        match e {
                            mpsc::TryRecvError::Empty => false,
                            mpsc::TryRecvError::Disconnected => {
                                // too many message the connection is lagging
                                true
                            }
                        }
                    }
                };

                if close && !closed {
                    if stop_laggy_client && !closed {
                        if let Err(e) = connection.close(true, 1, b"laggy client") {
                            if e != quiche::Error::Done {
                                log::error!("error closing client : {}", e);
                            }
                        } else {
                            log::info!(
                                "Stopping laggy client : {}",
                                connection.trace_id(),
                            );
                        }
                        closed = true;
                    }
                }
            }

            if instance.elapsed() > Duration::from_secs(2) {
                instance = Instant::now();
                connection.on_timeout();
            }

            loop {
                match connection.send(&mut out) {
                    Ok((len, send_info)) => {
                        sender.send((send_info, out[..len].to_vec())).unwrap();
                    }
                    Err(quiche::Error::Done) => {
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
    max_number_of_streams: u64,
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
                    let data = dispatching_connections_lk.get(id).unwrap();
                    data.message_counter
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if data.sender.send((binary.clone(), priority)).is_err() {
                        // client is closed
                        dispatching_connections_lk.remove(id);
                    }
                }
            }
            dispatching_connections_lk.retain(|_id, connection| {
                connection
                    .message_counter
                    .load(std::sync::atomic::Ordering::Relaxed)
                    < max_number_of_streams
            });
        }
    });
}

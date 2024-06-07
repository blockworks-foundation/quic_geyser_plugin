use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
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
    config::QuicParameters,
    defaults::{MAX_ALLOWED_PARTIAL_RESPONSES, MAX_DATAGRAM_SIZE},
    filters::Filter,
    message::Message,
    types::{account::Account, block_meta::SlotMeta, slot_identifier::SlotIdentifier},
};

use quic_geyser_quiche_utils::{
    quiche_reciever::{recv_message, ReadStreams},
    quiche_sender::{handle_writable, send_message},
    quiche_utils::{get_next_unidi, mint_token, validate_token, write_to_socket, PartialResponses},
};

use crate::configure_server::configure_server;

struct DispatchingData {
    pub sender: Sender<(Vec<u8>, u8)>,
    pub filters: Arc<RwLock<Vec<Filter>>>,
    pub messages_in_queue: Arc<AtomicUsize>,
}

type DispachingConnections = Arc<Mutex<HashMap<ConnectionId<'static>, DispatchingData>>>;

const MAX_BUFFER_SIZE: usize = 65535;

pub fn server_loop(
    quic_parameters: QuicParameters,
    socket_addr: SocketAddr,
    message_send_queue: mpsc::Receiver<ChannelMessage>,
    compression_type: CompressionType,
    stop_laggy_client: bool,
) -> anyhow::Result<()> {
    let maximum_concurrent_streams_id = u64::MAX;
    let mut config = configure_server(quic_parameters)?;

    let mut socket = mio::net::UdpSocket::bind(socket_addr)?;
    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(1024);

    let pacing = if quic_parameters.enable_pacing {
        match set_txtime_sockopt(&socket) {
            Ok(_) => {
                log::debug!("successfully set SO_TXTIME socket option");
                true
            }
            Err(e) => {
                log::debug!("setsockopt failed {:?}", e);
                false
            }
        }
    } else {
        false
    };

    poll.registry().register(
        &mut socket,
        mio::Token(0),
        mio::Interest::READABLE | mio::Interest::WRITABLE,
    )?;

    let mut buf = [0; MAX_BUFFER_SIZE];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    let local_addr = socket.local_addr()?;

    let enable_gso = if quic_parameters.enable_gso {
        detect_gso(&socket, MAX_DATAGRAM_SIZE)
    } else {
        false
    };

    let rng = SystemRandom::new();
    let conn_id_seed = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap();
    let mut clients: HashMap<
        quiche::ConnectionId<'static>,
        mio_channel::Sender<(quiche::RecvInfo, Vec<u8>)>,
    > = HashMap::new();

    let (write_sender, mut write_reciver) = mio_channel::channel::<(quiche::SendInfo, Vec<u8>)>();

    poll.registry()
        .register(&mut write_reciver, mio::Token(1), mio::Interest::READABLE)?;

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
        poll.poll(&mut events, Some(Duration::from_millis(10)))?;
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

                let (client_sender, client_reciver) = mio_channel::channel();
                let (client_message_sx, client_message_rx) = mpsc::channel();
                let messages_in_queue = Arc::new(AtomicUsize::new(0));

                let filters = Arc::new(RwLock::new(Vec::new()));
                create_client_task(
                    conn,
                    client_reciver,
                    write_sender.clone(),
                    client_message_rx,
                    filters.clone(),
                    maximum_concurrent_streams_id,
                    stop_laggy_client,
                    messages_in_queue.clone(),
                );
                let mut lk = dispatching_connections.lock().unwrap();
                lk.insert(
                    scid.clone(),
                    DispatchingData {
                        sender: client_message_sx,
                        filters,
                        messages_in_queue,
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

        while let Ok((send_info, buffer)) = write_reciver.try_recv() {
            if let Ok(written) = send_to(
                &socket,
                &buffer,
                &send_info,
                MAX_DATAGRAM_SIZE,
                pacing,
                enable_gso,
            ) {
                log::debug!("socket wrote to : {written:?} to {:?}", send_info.to);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn create_client_task(
    connection: quiche::Connection,
    mut receiver: mio_channel::Receiver<(quiche::RecvInfo, Vec<u8>)>,
    sender: mio_channel::Sender<(quiche::SendInfo, Vec<u8>)>,
    message_channel: mpsc::Receiver<(Vec<u8>, u8)>,
    filters: Arc<RwLock<Vec<Filter>>>,
    maximum_concurrent_streams_id: u64,
    stop_laggy_client: bool,
    messages_in_queue: Arc<AtomicUsize>,
) {
    std::thread::spawn(move || {
        let mut partial_responses = PartialResponses::new();
        let mut read_streams = ReadStreams::new();
        let mut next_stream: u64 = 3;
        let mut connection = connection;
        let mut instance = Instant::now();
        let mut closed = false;
        let mut buf = [0; MAX_DATAGRAM_SIZE];

        let mut poll = mio::Poll::new().unwrap();
        let mut events = mio::Events::with_capacity(1024);
        let max_allowed_partial_responses = MAX_ALLOWED_PARTIAL_RESPONSES as usize;

        poll.registry()
            .register(&mut receiver, mio::Token(0), mio::Interest::READABLE)
            .unwrap();

        let number_of_loops = Arc::new(AtomicU64::new(0));
        let number_of_meesages_from_network = Arc::new(AtomicU64::new(0));
        let number_of_meesages_to_network = Arc::new(AtomicU64::new(0));
        let number_of_readable_streams = Arc::new(AtomicU64::new(0));
        let number_of_writable_streams = Arc::new(AtomicU64::new(0));
        let messages_added = Arc::new(AtomicU64::new(0));
        let quit = Arc::new(AtomicBool::new(false));

        {
            let number_of_loops = number_of_loops.clone();
            let number_of_meesages_from_network = number_of_meesages_from_network.clone();
            let number_of_meesages_to_network = number_of_meesages_to_network.clone();
            let number_of_readable_streams = number_of_readable_streams.clone();
            let number_of_writable_streams = number_of_writable_streams.clone();
            let messages_added = messages_added.clone();
            let messages_in_queue = messages_in_queue.clone();
            let quit = quit.clone();
            std::thread::spawn(move || {
                while !quit.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_secs(1));
                    log::info!("---------------------------------");
                    log::info!(
                        "number of loop : {}",
                        number_of_loops.swap(0, std::sync::atomic::Ordering::Relaxed)
                    );
                    log::info!(
                        "number of packets read : {}",
                        number_of_meesages_from_network
                            .swap(0, std::sync::atomic::Ordering::Relaxed)
                    );
                    log::info!(
                        "number of packets write : {}",
                        number_of_meesages_to_network.swap(0, std::sync::atomic::Ordering::Relaxed)
                    );
                    log::info!(
                        "number_of_readable_streams : {}",
                        number_of_readable_streams.swap(0, std::sync::atomic::Ordering::Relaxed)
                    );
                    log::info!(
                        "number_of_writable_streams : {}",
                        number_of_writable_streams.swap(0, std::sync::atomic::Ordering::Relaxed)
                    );
                    log::info!(
                        "messages_added : {}",
                        messages_added.swap(0, std::sync::atomic::Ordering::Relaxed)
                    );
                    log::info!(
                        "messages in queue to be sent : {}",
                        messages_in_queue.load(std::sync::atomic::Ordering::Relaxed)
                    );
                }
            });
        }

        loop {
            number_of_loops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            poll.poll(&mut events, Some(Duration::from_millis(1)))
                .unwrap();

            if !events.is_empty() {
                while let Ok((info, mut buf)) = receiver.try_recv() {
                    let buf = buf.as_mut_slice();
                    match connection.recv(buf, info) {
                        Ok(_) => {
                            number_of_meesages_from_network
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                        Err(e) => {
                            log::error!("{} recv failed: {:?}", connection.trace_id(), e);
                            break;
                        }
                    };
                }
                continue;
            }

            if connection.is_in_early_data() || connection.is_established() {
                // Process all readable streams.
                for stream in connection.readable() {
                    number_of_readable_streams.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let message = recv_message(&mut connection, &mut read_streams, stream);
                    match message {
                        Ok(Some(message)) => match message {
                            Message::Filters(mut f) => {
                                let mut filter_lk = filters.write().unwrap();
                                filter_lk.append(&mut f);
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

            if !connection.is_closed()
                && (connection.is_established() || connection.is_in_early_data())
            {
                for stream_id in connection.writable() {
                    number_of_writable_streams.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if let Err(e) =
                        handle_writable(&mut connection, &mut partial_responses, stream_id)
                    {
                        log::error!("Error writing {e:?}");
                    }
                }

                while partial_responses.len() < max_allowed_partial_responses {
                    let close = match message_channel.try_recv() {
                        Ok((message, priority)) => {
                            messages_in_queue.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
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
                                true
                            } else {
                                messages_added.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                match send_message(
                                    &mut connection,
                                    &mut partial_responses,
                                    stream_id,
                                    &message,
                                ) {
                                    Ok(_) => false,
                                    Err(quiche::Error::Done) => {
                                        // done writing / queue is full
                                        break;
                                    }
                                    Err(e) => {
                                        log::error!("error sending message : {e:?}");
                                        true
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            match e {
                                mpsc::TryRecvError::Empty => {
                                    break;
                                }
                                mpsc::TryRecvError::Disconnected => {
                                    // too many message the connection is lagging
                                    log::error!("channel disconnected by dispatcher");
                                    true
                                }
                            }
                        }
                    };

                    if close && !closed && stop_laggy_client {
                        if let Err(e) = connection.close(true, 1, b"laggy client") {
                            if e != quiche::Error::Done {
                                log::error!("error closing client : {}", e);
                            }
                        } else {
                            log::info!("Stopping laggy client : {}", connection.trace_id(),);
                        }
                        closed = true;
                        break;
                    }
                }
            }

            if instance.elapsed() > Duration::from_secs(2) {
                instance = Instant::now();
                connection.on_timeout();
            }

            let max_burst = connection.send_quantum();
            let mut total_send = 0;
            while total_send < max_burst {
                match connection.send(&mut buf[0..MAX_DATAGRAM_SIZE]) {
                    Ok((len, send_info)) => {
                        sender.send((send_info, buf[..len].to_vec())).unwrap();
                        number_of_meesages_to_network
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        total_send += len;
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
        quit.store(true, std::sync::atomic::Ordering::Relaxed);
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

                        (Message::AccountMsg(geyser_account), 3)
                    }
                    ChannelMessage::Slot(slot, parent, commitment_config) => (
                        Message::SlotMsg(SlotMeta {
                            slot,
                            parent,
                            commitment_config,
                        }),
                        1,
                    ),
                    ChannelMessage::BlockMeta(block_meta) => (Message::BlockMetaMsg(block_meta), 2),
                    ChannelMessage::Transaction(transaction) => {
                        (Message::TransactionMsg(transaction), 3)
                    }
                    ChannelMessage::Block(block) => (Message::BlockMsg(block), 2),
                };
                let binary =
                    bincode::serialize(&message).expect("Message should be serializable in binary");
                for id in dispatching_connections.iter() {
                    let data = dispatching_connections_lk.get(id).unwrap();
                    data.messages_in_queue
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if data.sender.send((binary.clone(), priority)).is_err() {
                        // client is closed
                        dispatching_connections_lk.remove(id);
                    }
                }
            }
        }
    });
}

fn set_txtime_sockopt(sock: &mio::net::UdpSocket) -> std::io::Result<()> {
    use nix::sys::socket::setsockopt;
    use nix::sys::socket::sockopt::TxTime;
    use std::os::unix::io::AsRawFd;

    let config = nix::libc::sock_txtime {
        clockid: libc::CLOCK_MONOTONIC,
        flags: 0,
    };

    // mio::net::UdpSocket doesn't implement AsFd (yet?).
    let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(sock.as_raw_fd()) };

    setsockopt(&fd, TxTime, &config)?;

    Ok(())
}

fn std_time_to_u64(time: &std::time::Instant) -> u64 {
    const NANOS_PER_SEC: u64 = 1_000_000_000;

    const INSTANT_ZERO: std::time::Instant = unsafe { std::mem::transmute(std::time::UNIX_EPOCH) };

    let raw_time = time.duration_since(INSTANT_ZERO);

    let sec = raw_time.as_secs();
    let nsec = raw_time.subsec_nanos();

    sec * NANOS_PER_SEC + nsec as u64
}

pub fn detect_gso(socket: &mio::net::UdpSocket, segment_size: usize) -> bool {
    use nix::sys::socket::setsockopt;
    use nix::sys::socket::sockopt::UdpGsoSegment;
    use std::os::unix::io::AsRawFd;

    // mio::net::UdpSocket doesn't implement AsFd (yet?).
    let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(socket.as_raw_fd()) };

    setsockopt(&fd, UdpGsoSegment, &(segment_size as i32)).is_ok()
}

fn send_to_gso_pacing(
    socket: &mio::net::UdpSocket,
    buf: &[u8],
    send_info: &quiche::SendInfo,
    segment_size: usize,
) -> std::io::Result<usize> {
    use nix::sys::socket::sendmsg;
    use nix::sys::socket::ControlMessage;
    use nix::sys::socket::MsgFlags;
    use nix::sys::socket::SockaddrStorage;
    use std::io::IoSlice;
    use std::os::unix::io::AsRawFd;

    let iov = [IoSlice::new(buf)];
    let segment_size = segment_size as u16;
    let dst = SockaddrStorage::from(send_info.to);
    let sockfd = socket.as_raw_fd();

    // GSO option.
    let cmsg_gso = ControlMessage::UdpGsoSegments(&segment_size);

    // Pacing option.
    let send_time = std_time_to_u64(&send_info.at);
    let cmsg_txtime = ControlMessage::TxTime(&send_time);

    match sendmsg(
        sockfd,
        &iov,
        &[cmsg_gso, cmsg_txtime],
        MsgFlags::empty(),
        Some(&dst),
    ) {
        Ok(v) => Ok(v),
        Err(e) => Err(e.into()),
    }
}

pub fn send_to(
    socket: &mio::net::UdpSocket,
    buf: &[u8],
    send_info: &quiche::SendInfo,
    segment_size: usize,
    pacing: bool,
    enable_gso: bool,
) -> std::io::Result<usize> {
    if pacing && enable_gso {
        match send_to_gso_pacing(socket, buf, send_info, segment_size) {
            Ok(v) => {
                return Ok(v);
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    let mut off = 0;
    let mut left = buf.len();
    let mut written = 0;
    while left > 0 {
        let pkt_len = std::cmp::min(left, segment_size);
        match socket.send_to(&buf[off..off + pkt_len], send_info.to) {
            Ok(v) => {
                written += v;
            }
            Err(e) => return Err(e),
        }
        off += pkt_len;
        left -= pkt_len;
    }

    Ok(written)
}

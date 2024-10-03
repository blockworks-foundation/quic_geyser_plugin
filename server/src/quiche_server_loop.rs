use std::{
    collections::HashMap,
    net::{SocketAddr, UdpSocket},
    sync::{
        atomic::AtomicBool,
        mpsc::{self, Sender},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant},
    u64,
};

use anyhow::bail;
use itertools::Itertools;
use nix::sys::socket::sockopt::ReusePort;
use prometheus::{opts, register_int_gauge, IntGauge};
use quiche::ConnectionId;
use ring::rand::SystemRandom;

use quic_geyser_common::{
    channel_message::ChannelMessage,
    compression::CompressionType,
    config::QuicParameters,
    defaults::MAX_DATAGRAM_SIZE,
    filters::Filter,
    message::Message,
    types::{account::Account, block_meta::SlotMeta, slot_identifier::SlotIdentifier},
};

use quic_geyser_quiche_utils::{
    quiche_reciever::{recv_message, ReadStreams},
    quiche_sender::{handle_writable, send_message},
    quiche_utils::{
        generate_cid_and_reset_token, get_next_unidi, handle_path_events, mint_token,
        validate_token, StreamSenderMap,
    },
};

lazy_static::lazy_static! {
    static ref NUMBER_OF_CLIENTS: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_connection", "Number of connections")).unwrap();

    static ref NUMBER_OF_CONNECTION_CLOSED: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_connection_closed", "Number of connection closed")).unwrap();

    static ref NUMBER_OF_BYTES_SENT: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_bytes_sent", "Number of bytes sent")).unwrap();

    static ref NUMBER_OF_BYTES_READ: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_bytes_read", "Number of bytes sent")).unwrap();

    static ref NUMBER_OF_WRITE_COUNT: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_write_count", "Number of bytes sent")).unwrap();

    static ref NUMBER_OF_READ_COUNT: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_read_count", "Number of account updates")).unwrap();

    static ref NUMBER_OF_ACCOUNT_UPDATES: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_account_updates", "Number of account updates")).unwrap();

    static ref NUMBER_OF_TRANSACTION_UPDATES: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_transaction_updates", "Number of transaction updates")).unwrap();

    static ref NUMBER_OF_SLOT_UPDATES: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_slot_updates", "Number of slot updates")).unwrap();

    static ref NUMBER_OF_BLOCKMETA_UPDATE: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_blockmeta_updates", "Number of blockmeta updates")).unwrap();

    static ref NUMBER_OF_BLOCK_UPDATES: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_block_updates", "Number of block updates")).unwrap();
}

use crate::configure_server::configure_server;

enum InternalMessage {
    Packet(quiche::RecvInfo, Vec<u8>),
    ClientMessage(Vec<u8>, u8),
}

struct DispatchingData {
    pub sender: Sender<InternalMessage>,
    pub filters: Arc<RwLock<Vec<Filter>>>,
}

type DispachingConnections = Arc<Mutex<HashMap<ConnectionId<'static>, DispatchingData>>>;

pub fn server_loop(
    quic_params: QuicParameters,
    socket_addr: SocketAddr,
    message_send_queue: mpsc::Receiver<ChannelMessage>,
    compression_type: CompressionType,
    stop_laggy_client: bool,
) -> anyhow::Result<()> {
    let maximum_concurrent_streams = 32;
    let mut config = configure_server(quic_params)?;

    let socket = Arc::new(UdpSocket::bind(socket_addr)?);
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    let local_addr = socket.local_addr()?;
    let rng = SystemRandom::new();
    let conn_id_seed = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap();
    let mut client_messsage_channel_by_id: HashMap<u64, mpsc::Sender<InternalMessage>> =
        HashMap::new();
    let clients_by_id: Arc<Mutex<HashMap<ConnectionId<'static>, u64>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let enable_pacing = if quic_params.enable_pacing {
        set_txtime_sockopt(&socket).is_ok()
    } else {
        false
    };

    let enable_gso = if quic_params.enable_gso {
        detect_gso(&socket, MAX_DATAGRAM_SIZE)
    } else {
        false
    };

    log::info!("pacing is enabled on server : {enable_pacing:?}");

    let dispatching_connections: DispachingConnections = Arc::new(Mutex::new(HashMap::<
        ConnectionId<'static>,
        DispatchingData,
    >::new()));

    create_dispatching_thread(
        message_send_queue,
        dispatching_connections.clone(),
        compression_type,
    );
    let mut client_id_counter = 0;

    loop {
        let (len, from) = match socket.recv_from(&mut buf) {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    break;
                }
                bail!("recv() failed: {:?}", e);
            }
        };

        NUMBER_OF_BYTES_READ.add(len as i64);
        NUMBER_OF_READ_COUNT.inc();
        let pkt_buf = &mut buf[..len];

        // Parse the QUIC packet's header.
        let hdr = match quiche::Header::from_slice(pkt_buf, quiche::MAX_CONN_ID_LEN) {
            Ok(v) => v,

            Err(e) => {
                log::error!("Parsing packet header failed: {:?}", e);
                continue;
            }
        };

        let conn_id = ring::hmac::sign(&conn_id_seed, &hdr.dcid);
        let conn_id = &conn_id.as_ref()[..quiche::MAX_CONN_ID_LEN];
        let conn_id: ConnectionId<'static> = conn_id.to_vec().into();
        let mut clients_lk = clients_by_id.lock().unwrap();
        if !clients_lk.contains_key(&hdr.dcid) && !clients_lk.contains_key(&conn_id) {
            drop(clients_lk);
            if hdr.ty != quiche::Type::Initial {
                log::error!("Packet is not Initial");
                continue;
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
                continue;
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

                if let Err(e) = socket.send_to(&out[..len], from) {
                    log::error!("Error sending retry messages : {e:?}");
                }
                continue;
            }

            let odcid = validate_token(&from, token);

            if odcid.is_none() {
                log::error!("Invalid address validation token");
                continue;
            }

            if scid.len() != hdr.dcid.len() {
                log::error!("Invalid destination connection ID");
                continue;
            }

            let scid = hdr.dcid.clone();

            log::info!("New connection: dcid={:?} scid={:?}", hdr.dcid, scid);

            let mut conn = quiche::accept(&scid, odcid.as_ref(), local_addr, from, &mut config)?;

            let recv_info = quiche::RecvInfo {
                to: socket.local_addr().unwrap(),
                from,
            };
            // Process potentially coalesced packets.
            match conn.recv(pkt_buf, recv_info) {
                Ok(v) => v,
                Err(e) => {
                    log::error!("{} recv failed: {:?}", conn.trace_id(), e);
                    continue;
                }
            };

            let (client_message_sx, client_message_rx) = mpsc::channel();
            let current_client_id = client_id_counter;
            client_id_counter += 1;

            let filters = Arc::new(RwLock::new(Vec::new()));
            create_client_task(
                socket.clone(),
                conn,
                current_client_id,
                clients_by_id.clone(),
                client_message_rx,
                filters.clone(),
                maximum_concurrent_streams,
                stop_laggy_client,
                quic_params.incremental_priority,
                rng.clone(),
                enable_pacing,
                enable_gso,
            );
            let mut lk = dispatching_connections.lock().unwrap();
            lk.insert(
                scid.clone(),
                DispatchingData {
                    sender: client_message_sx.clone(),
                    filters,
                },
            );
            let mut clients_lk = clients_by_id.lock().unwrap();
            clients_lk.insert(scid, current_client_id);
            client_messsage_channel_by_id.insert(current_client_id, client_message_sx);
        } else {
            // get the existing client
            let client_id = match clients_lk.get(&hdr.dcid) {
                Some(v) => *v,
                None => *clients_lk
                    .get(&conn_id)
                    .expect("The client should exist in the map"),
            };

            let recv_info = quiche::RecvInfo {
                to: socket.local_addr().unwrap(),
                from,
            };
            match client_messsage_channel_by_id.get_mut(&client_id) {
                Some(channel) => {
                    if channel
                        .send(InternalMessage::Packet(recv_info, pkt_buf.to_vec()))
                        .is_err()
                    {
                        // client is closed
                        clients_lk.remove(&hdr.dcid);
                        clients_lk.remove(&conn_id);
                        client_messsage_channel_by_id.remove(&client_id);
                    }
                }
                None => {
                    log::error!("channel with client id {client_id} not found");
                }
            }
        };
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn create_client_task(
    socket: Arc<UdpSocket>,
    connection: quiche::Connection,
    client_id: u64,
    client_id_by_scid: Arc<Mutex<HashMap<ConnectionId<'static>, u64>>>,
    receiver: mpsc::Receiver<InternalMessage>,
    filters: Arc<RwLock<Vec<Filter>>>,
    maximum_concurrent_streams: usize,
    stop_laggy_client: bool,
    incremental_priority: bool,
    rng: SystemRandom,
    enable_pacing: bool,
    enable_gso: bool,
) {
    std::thread::spawn(move || {
        let mut stream_sender_map = StreamSenderMap::new();
        let mut read_streams = ReadStreams::new();
        let first_stream_id = get_next_unidi(0, true, u64::MAX);
        let mut next_stream: u64 = first_stream_id;
        let mut connection = connection;
        let mut instance = Instant::now();
        let mut closed = false;
        let mut out = [0; 65535];
        let mut datagram_size = MAX_DATAGRAM_SIZE;
        let mut logged_is_draining = false;
        let quit = Arc::new(AtomicBool::new(false));

        let mut continue_write = true;
        NUMBER_OF_CLIENTS.inc();
        loop {
            let mut timeout = if continue_write {
                Duration::from_secs(0)
            } else {
                connection.timeout().unwrap_or(Duration::from_secs(1))
            };

            let mut did_read = false;

            while let Ok(internal_message) = receiver.recv_timeout(timeout) {
                did_read = true;
                timeout = Duration::from_secs(0);

                match internal_message {
                    InternalMessage::Packet(info, mut buf) => {
                        // handle packet from udp socket
                        let buf = buf.as_mut_slice();
                        match connection.recv(buf, info) {
                            Ok(_) => {}
                            Err(e) => {
                                log::error!("{} recv failed: {:?}", connection.trace_id(), e);
                                break;
                            }
                        };
                    }
                    InternalMessage::ClientMessage(message, priority) => {
                        // handle message from client
                        // create new stream is there are still streams to be used or else use the existing one with most capacity
                        let stream_id = if stream_sender_map.len() < maximum_concurrent_streams {
                            let stream_id_to_use = next_stream;
                            next_stream = get_next_unidi(stream_id_to_use, true, u64::MAX);
                            log::debug!("Creating new stream to use :{stream_id_to_use}");
                            if stream_id_to_use == first_stream_id {
                                // set high priority to first stream
                                connection
                                    .stream_priority(stream_id_to_use, 0, incremental_priority)
                                    .unwrap();
                            } else {
                                connection
                                    .stream_priority(stream_id_to_use, 1, incremental_priority)
                                    .unwrap();
                            }
                            stream_id_to_use
                        } else {
                            // for high priority streams
                            let stream_id = if priority == 0 {
                                first_stream_id
                            } else {
                                let value = stream_sender_map
                                    .iter()
                                    .max_by(|x, y| x.1.capacity().cmp(&y.1.capacity()))
                                    .unwrap()
                                    .0;
                                *value
                            };
                            log::debug!("Reusing stream {stream_id}");
                            stream_id
                        };

                        let close = match send_message(
                            &mut connection,
                            &mut stream_sender_map,
                            stream_id,
                            message,
                        ) {
                            Ok(_) => {
                                // do nothing
                                false
                            }
                            Err(e) => {
                                // done writing / queue is full
                                log::error!("got error sending message client : {}", e);
                                true
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
            }

            if !did_read && !continue_write {
                connection.on_timeout();
            }
            continue_write = false;

            if connection.is_in_early_data() || connection.is_established() {
                // Process all readable streams.
                for stream in connection.readable() {
                    let message = recv_message(&mut connection, &mut read_streams, stream);
                    match message {
                        Ok(Some(messages)) => {
                            log::info!("messages : {messages:?}");
                            let mut filter_lk = filters.write().unwrap();
                            for message in messages {
                                match message {
                                    Message::Filters(mut f) => {
                                        filter_lk.append(&mut f);
                                    }
                                    Message::Ping => {
                                        log::debug!("recieved ping from the client");
                                    }
                                    _ => {
                                        log::error!("unknown message from the client");
                                    }
                                }
                            }
                        }
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
                datagram_size = connection.max_send_udp_payload_size();
                for stream_id in connection.writable() {
                    if let Err(e) =
                        handle_writable(&mut connection, &mut stream_sender_map, stream_id)
                    {
                        if e == quiche::Error::Done {
                            break;
                        }
                        if !closed {
                            log::error!("Error writing {e:?}");
                            let _ = connection.close(true, 1, b"stream stopped");
                            closed = true;
                            break;
                        }
                    }
                }
            }

            if instance.elapsed() > Duration::from_secs(1) {
                log::debug!("other tasks");
                instance = Instant::now();
                handle_path_events(&mut connection);

                // See whether source Connection IDs have been retired.
                while let Some(retired_scid) = connection.retired_scid_next() {
                    log::info!("Retiring source CID {:?}", retired_scid);
                    client_id_by_scid.lock().unwrap().remove(&retired_scid);
                }

                // Provides as many CIDs as possible.
                while connection.scids_left() > 0 {
                    let (scid, reset_token) = generate_cid_and_reset_token(&rng);

                    log::info!("providing new scid {scid:?}");
                    if connection.new_scid(&scid, reset_token, false).is_err() {
                        break;
                    }
                    client_id_by_scid.lock().unwrap().insert(scid, client_id);
                }
            }

            let mut send_message_to = None;
            let mut total_length = 0;

            let max_burst_size = if enable_gso {
                std::cmp::min(datagram_size * 10, out.len())
            } else {
                datagram_size
            };

            while total_length < max_burst_size {
                match connection.send(&mut out[total_length..max_burst_size]) {
                    Ok((len, send_info)) => {
                        send_message_to.get_or_insert(send_info);
                        total_length += len;
                        if len < datagram_size {
                            break;
                        }
                    }
                    Err(quiche::Error::BufferTooShort) => {
                        // retry later
                        log::trace!("{} buffer to short", connection.trace_id());
                        break;
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
                    }
                };
            }

            if total_length > 0 && send_message_to.is_some() {
                log::debug!("sending :{total_length:?}");
                continue_write = true;
                NUMBER_OF_BYTES_SENT.add(total_length as i64);
                NUMBER_OF_WRITE_COUNT.inc();
                let send_result = if enable_pacing {
                    send_with_pacing(
                        &socket,
                        &out[..total_length],
                        &send_message_to.unwrap(),
                        enable_gso,
                        datagram_size as u16,
                    )
                } else {
                    socket.send(&out[..total_length])
                };
                match send_result {
                    Ok(_written) => {
                        log::debug!("finished sending");
                    }
                    Err(e) => {
                        log::error!("sending failed with error : {e:?}");
                    }
                }
            }

            if !logged_is_draining && connection.is_draining() {
                log::warn!("connection is draining");
                logged_is_draining = true;
            }

            if connection.is_closed() {
                NUMBER_OF_CONNECTION_CLOSED.inc();
                if let Some(e) = connection.peer_error() {
                    log::error!("peer error : {e:?} ");
                }

                if let Some(e) = connection.local_error() {
                    log::error!("local error : {e:?} ");
                }

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
    NUMBER_OF_CLIENTS.dec();
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
                    ChannelMessage::Account(account, slot, init) => {
                        if init {
                            // do not sent init messages
                            continue;
                        }
                        NUMBER_OF_ACCOUNT_UPDATES.inc();
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
                    ChannelMessage::Slot(slot, parent, commitment_config) => {
                        NUMBER_OF_SLOT_UPDATES.inc();
                        (
                            Message::SlotMsg(SlotMeta {
                                slot,
                                parent,
                                commitment_config,
                            }),
                            0,
                        )
                    }
                    ChannelMessage::BlockMeta(block_meta) => {
                        NUMBER_OF_BLOCKMETA_UPDATE.inc();
                        (Message::BlockMetaMsg(block_meta), 0)
                    }
                    ChannelMessage::Transaction(transaction) => {
                        NUMBER_OF_TRANSACTION_UPDATES.inc();
                        (Message::TransactionMsg(transaction), 3)
                    }
                    ChannelMessage::Block(block) => {
                        NUMBER_OF_BLOCK_UPDATES.inc();
                        (Message::BlockMsg(block), 2)
                    }
                };
                let binary = message.to_binary_stream();
                for id in dispatching_connections.iter() {
                    let data = dispatching_connections_lk.get(id).unwrap();
                    if data
                        .sender
                        .send(InternalMessage::ClientMessage(binary.clone(), priority))
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

fn set_txtime_sockopt(sock: &UdpSocket) -> std::io::Result<()> {
    use nix::sys::socket::setsockopt;
    use nix::sys::socket::sockopt::TxTime;
    use std::os::unix::io::AsRawFd;

    let config = nix::libc::sock_txtime {
        clockid: libc::CLOCK_MONOTONIC,
        flags: 0,
    };

    let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(sock.as_raw_fd()) };

    setsockopt(&fd, TxTime, &config)?;
    setsockopt(&fd, ReusePort, &true)?;
    Ok(())
}

const NANOS_PER_SEC: u64 = 1_000_000_000;

const INSTANT_ZERO: std::time::Instant = unsafe { std::mem::transmute(std::time::UNIX_EPOCH) };

fn std_time_to_u64(time: &std::time::Instant) -> u64 {
    let raw_time = time.duration_since(INSTANT_ZERO);
    let sec = raw_time.as_secs();
    let nsec = raw_time.subsec_nanos();
    sec * NANOS_PER_SEC + nsec as u64
}

fn send_with_pacing(
    socket: &UdpSocket,
    buf: &[u8],
    send_info: &quiche::SendInfo,
    enable_gso: bool,
    segment_size: u16,
) -> std::io::Result<usize> {
    use nix::sys::socket::sendmsg;
    use nix::sys::socket::ControlMessage;
    use nix::sys::socket::MsgFlags;
    use nix::sys::socket::SockaddrStorage;
    use std::io::IoSlice;
    use std::os::unix::io::AsRawFd;

    let iov = [IoSlice::new(buf)];
    let dst = SockaddrStorage::from(send_info.to);
    let sockfd = socket.as_raw_fd();

    // Pacing option.
    let send_time = std_time_to_u64(&send_info.at);
    let cmsg_txtime = ControlMessage::TxTime(&send_time);

    let mut cmgs = vec![cmsg_txtime];
    if enable_gso {
        cmgs.push(ControlMessage::UdpGsoSegments(&segment_size));
    }

    match sendmsg(sockfd, &iov, &cmgs, MsgFlags::empty(), Some(&dst)) {
        Ok(v) => Ok(v),
        Err(e) => Err(e.into()),
    }
}

pub fn detect_gso(socket: &UdpSocket, segment_size: usize) -> bool {
    use nix::sys::socket::setsockopt;
    use nix::sys::socket::sockopt::UdpGsoSegment;
    use std::os::unix::io::AsRawFd;

    // mio::net::UdpSocket doesn't implement AsFd (yet?).
    let fd = unsafe { std::os::fd::BorrowedFd::borrow_raw(socket.as_raw_fd()) };

    setsockopt(&fd, UdpGsoSegment, &(segment_size as i32)).is_ok()
}

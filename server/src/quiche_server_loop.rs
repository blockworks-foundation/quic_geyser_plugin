use std::{collections::HashMap, net::SocketAddr};

use itertools::Itertools;
use quiche::ConnectionId;
use ring::rand::{SecureRandom, SystemRandom};

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
    quiche_utils::{get_next_unidi, mint_token, validate_token, PartialResponses},
};

use crate::configure_server::configure_server;

const MAX_BUFFER_SIZE: usize = 65507;
const MAX_MESSAGES_PER_LOOP: usize = 2;

pub struct Client {
    pub conn: quiche::Connection,
    pub client_id: ClientId,
    pub partial_requests: ReadStreams,
    pub partial_responses: PartialResponses,
    pub max_datagram_size: usize,
    pub loss_rate: f64,
    pub max_send_burst: usize,
    pub max_burst_was_set: bool,
    pub filters: Vec<Filter>,
    pub next_stream: u64,
    pub closed: bool,
}
pub type ClientId = u64;
pub type ClientIdMap = HashMap<ConnectionId<'static>, ClientId>;
pub type ClientMap = HashMap<ClientId, Client>;

pub fn server_loop(
    mut quic_parameters: QuicParameters,
    socket_addr: SocketAddr,
    mut message_send_queue: mio_channel::Receiver<ChannelMessage>,
    compression_type: CompressionType,
    stop_laggy_client: bool,
) -> anyhow::Result<()> {
    let maximum_concurrent_streams_id = u64::MAX;

    let mut buf = [0; MAX_BUFFER_SIZE];
    let mut out = [0; MAX_BUFFER_SIZE];
    let mut pacing = false;
    // Setup the event loop.
    let mut poll = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(1024);

    // Create the UDP listening socket, and register it with the event loop.
    let mut socket = mio::net::UdpSocket::bind(socket_addr)?;

    poll.registry()
        .register(&mut socket, mio::Token(0), mio::Interest::READABLE)
        .unwrap();

    poll.registry()
        .register(
            &mut message_send_queue,
            mio::Token(1),
            mio::Interest::READABLE,
        )
        .unwrap();

    let max_datagram_size = MAX_DATAGRAM_SIZE;
    let enable_gso = if quic_parameters.enable_gso {
        detect_gso(&socket, max_datagram_size)
    } else {
        false
    };

    if !quic_parameters.enable_pacing {
        match set_txtime_sockopt(&socket) {
            Ok(_) => {
                pacing = true;
                log::debug!("successfully set SO_TXTIME socket option");
            }
            Err(e) => log::debug!("setsockopt failed {:?}", e),
        };
    }
    quic_parameters.enable_pacing = pacing;

    let mut config = configure_server(quic_parameters)?;

    let rng = SystemRandom::new();
    let conn_id_seed = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap();
    let mut next_client_id = 0;
    let mut clients_ids = ClientIdMap::new();
    let mut clients = ClientMap::new();

    let mut continue_write = false;
    let local_addr = socket.local_addr().unwrap();

    loop {
        // Find the shorter timeout from all the active connections.
        //
        // TODO: use event loop that properly supports timers
        let timeout = match continue_write {
            true => Some(std::time::Duration::from_secs(0)),

            false => clients.values().filter_map(|c| c.conn.timeout()).min(),
        };

        poll.poll(&mut events, timeout).unwrap();

        // Read incoming UDP packets from the socket and feed them to quiche,
        // until there are no more packets to read.
        'read: loop {
            // If the event loop reported no events, it means that the timeout
            // has expired, so handle it without attempting to read packets. We
            // will then proceed with the send loop.
            if events.is_empty() && !continue_write {
                log::trace!("timed out");

                clients.values_mut().for_each(|c| c.conn.on_timeout());

                break 'read;
            }

            let (len, from) = match socket.recv_from(&mut buf) {
                Ok(v) => v,

                Err(e) => {
                    // There are no more UDP packets to read, so end the read
                    // loop.
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        log::trace!("recv() would block");
                        break 'read;
                    }

                    panic!("recv() failed: {:?}", e);
                }
            };

            log::trace!("got {} bytes", len);

            let pkt_buf = &mut buf[..len];
            // Parse the QUIC packet's header.
            let hdr = match quiche::Header::from_slice(pkt_buf, quiche::MAX_CONN_ID_LEN) {
                Ok(v) => v,

                Err(e) => {
                    log::error!("Parsing packet header failed: {:?}", e);
                    continue 'read;
                }
            };

            log::trace!("got packet {:?}", hdr);

            let conn_id = if !cfg!(feature = "fuzzing") {
                let conn_id = ring::hmac::sign(&conn_id_seed, &hdr.dcid);
                let conn_id = &conn_id.as_ref()[..quiche::MAX_CONN_ID_LEN];
                conn_id.to_vec().into()
            } else {
                // When fuzzing use an all zero connection ID.
                [0; quiche::MAX_CONN_ID_LEN].to_vec().into()
            };

            // Lookup a connection based on the packet's connection ID. If there
            // is no connection matching, create a new one.
            let client = if !clients_ids.contains_key(&hdr.dcid)
                && !clients_ids.contains_key(&conn_id)
            {
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
                            log::trace!("send() would block");
                            break;
                        }

                        panic!("send() failed: {:?}", e);
                    }
                    continue 'read;
                }

                let mut scid = [0; quiche::MAX_CONN_ID_LEN];
                scid.copy_from_slice(&conn_id);

                #[allow(unused_assignments)]
                let mut odcid = None;

                {
                    // Token is always present in Initial packets.
                    let token = hdr.token.as_ref().unwrap();

                    // Do stateless retry if the client didn't send a token.
                    if token.is_empty() {
                        log::warn!("Doing stateless retry");

                        let scid = quiche::ConnectionId::from_ref(&scid);
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
                                log::trace!("send() would block");
                                break;
                            }

                            panic!("send() failed: {:?}", e);
                        }
                        continue 'read;
                    }

                    odcid = validate_token(&from, token);

                    // The token was not valid, meaning the retry failed, so
                    // drop the packet.
                    if odcid.is_none() {
                        log::error!("Invalid address validation token");
                        continue;
                    }

                    if scid.len() != hdr.dcid.len() {
                        log::error!("Invalid destination connection ID");
                        continue 'read;
                    }

                    // Reuse the source connection ID we sent in the Retry
                    // packet, instead of changing it again.
                    scid.copy_from_slice(&hdr.dcid);
                }

                let scid = quiche::ConnectionId::from_vec(scid.to_vec());

                log::debug!("New connection: dcid={:?} scid={:?}", hdr.dcid, scid);

                #[allow(unused_mut)]
                let mut conn =
                    quiche::accept(&scid, odcid.as_ref(), local_addr, from, &mut config).unwrap();

                let client_id = next_client_id;

                let client = Client {
                    conn,
                    client_id,
                    partial_requests: HashMap::new(),
                    partial_responses: HashMap::new(),
                    max_burst_was_set: false,
                    max_datagram_size,
                    loss_rate: 0.0,
                    max_send_burst: MAX_BUFFER_SIZE,
                    filters: vec![],
                    next_stream: 0,
                    closed: false,
                };

                clients.insert(client_id, client);
                clients_ids.insert(scid.clone(), client_id);

                next_client_id += 1;

                clients.get_mut(&client_id).unwrap()
            } else {
                let cid = match clients_ids.get(&hdr.dcid) {
                    Some(v) => v,

                    None => clients_ids.get(&conn_id).unwrap(),
                };

                clients.get_mut(cid).unwrap()
            };

            let recv_info = quiche::RecvInfo {
                to: local_addr,
                from,
            };

            // Process potentially coalesced packets.
            let read = match client.conn.recv(pkt_buf, recv_info) {
                Ok(v) => v,

                Err(e) => {
                    log::error!("{} recv failed: {:?}", client.conn.trace_id(), e);
                    continue 'read;
                }
            };

            log::debug!("{} processed {} bytes", client.conn.trace_id(), read);

            // Create a new application protocol session as soon as the QUIC
            // connection is established.
            if !client.max_burst_was_set
                && (client.conn.is_in_early_data() || client.conn.is_established())
            {
                client.max_burst_was_set = true;
                // Update max_datagram_size after connection established.
                client.max_datagram_size = client.conn.max_send_udp_payload_size();
            }

            let conn = &mut client.conn;
            let partial_responses = &mut client.partial_responses;
            let read_streams = &mut client.partial_requests;
            let filters = &mut client.filters;
            // Handle writable streams.
            for stream_id in conn.writable() {
                if let Err(e) = handle_writable(conn, partial_responses, stream_id) {
                    log::error!("Error writing {e:?}");
                }
            }

            for stream in conn.readable() {
                let message = recv_message(conn, read_streams, stream);
                match message {
                    Ok(Some(message)) => match message {
                        Message::Filters(mut f) => {
                            log::debug!("filters added ..");
                            filters.append(&mut f);
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

            handle_path_events(client);

            // See whether source Connection IDs have been retired.
            while let Some(retired_scid) = client.conn.retired_scid_next() {
                log::info!("Retiring source CID {:?}", retired_scid);
                clients_ids.remove(&retired_scid);
            }

            // Provides as many CIDs as possible.
            while client.conn.scids_left() > 0 {
                let (scid, reset_token) = generate_cid_and_reset_token(&rng);
                if client.conn.new_scid(&scid, reset_token, false).is_err() {
                    break;
                }

                clients_ids.insert(scid, client.client_id);
            }
        }

        for _ in 0..MAX_MESSAGES_PER_LOOP {
            if let Ok(message) = message_send_queue.try_recv() {
                let dispatching_connections = clients
                    .iter()
                    .filter_map(|(id, client)| {
                        if client.filters.iter().any(|x| x.allows(&message)) {
                            Some(*id)
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
                        ChannelMessage::BlockMeta(block_meta) => {
                            (Message::BlockMetaMsg(block_meta), 2)
                        }
                        ChannelMessage::Transaction(transaction) => {
                            (Message::TransactionMsg(transaction), 3)
                        }
                        ChannelMessage::Block(block) => (Message::BlockMsg(block), 2),
                    };
                    let binary = bincode::serialize(&message)
                        .expect("Message should be serializable in binary");
                    for id in dispatching_connections.iter() {
                        let client = clients.get_mut(id).unwrap();
                        client.next_stream =
                            get_next_unidi(client.next_stream, true, maximum_concurrent_streams_id);

                        let stream_id = client.next_stream;

                        if let Err(e) = client.conn.stream_priority(stream_id, priority, true) {
                            if !client.closed && stop_laggy_client {
                                client.closed = true;
                                log::error!("Error setting stream : {e:?}");
                                let _ = client.conn.close(true, 0, b"laggy");
                            }
                        } else {
                            log::debug!("sending message to {id}");
                            match send_message(
                                &mut client.conn,
                                &mut client.partial_responses,
                                stream_id,
                                &binary,
                            ) {
                                Ok(_) => {}
                                Err(quiche::Error::Done) => {
                                    // done writing / queue is full
                                    break;
                                }
                                Err(e) => {
                                    if !client.closed && stop_laggy_client {
                                        client.closed = true;
                                        log::error!("Error sending message : {e:?}");
                                        let _ = client.conn.close(true, 0, b"laggy");
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                break;
            }
        }

        // Generate outgoing QUIC packets for all active connections and send
        // them on the UDP socket, until quiche reports that there are no more
        // packets to be sent.
        continue_write = false;
        for client in clients.values_mut() {
            // Reduce max_send_burst by 25% if loss is increasing more than 0.1%.
            let loss_rate = client.conn.stats().lost as f64 / client.conn.stats().sent as f64;
            if loss_rate > client.loss_rate + 0.001 {
                client.max_send_burst = client.max_send_burst / 4 * 3;
                // Minimun bound of 10xMSS.
                client.max_send_burst = client.max_send_burst.max(client.max_datagram_size * 10);
                client.loss_rate = loss_rate;
            }

            let max_send_burst = client.conn.send_quantum().min(client.max_send_burst)
                / client.max_datagram_size
                * client.max_datagram_size;
            let mut total_write = 0;
            let mut dst_info = None;

            while total_write < max_send_burst {
                let (write, send_info) =
                    match client.conn.send(&mut out[total_write..max_send_burst]) {
                        Ok(v) => v,

                        Err(quiche::Error::Done) => {
                            log::trace!("{} done writing", client.conn.trace_id());
                            break;
                        }

                        Err(e) => {
                            log::error!("{} send failed: {:?}", client.conn.trace_id(), e);

                            client.conn.close(false, 0x1, b"fail").ok();
                            break;
                        }
                    };

                total_write += write;

                // Use the first packet time to send, not the last.
                let _ = dst_info.get_or_insert(send_info);

                if write < client.max_datagram_size {
                    continue_write = true;
                    break;
                }
            }

            if total_write == 0 || dst_info.is_none() {
                break;
            }

            if let Err(e) = send_to(
                &socket,
                &out[..total_write],
                &dst_info.unwrap(),
                client.max_datagram_size,
                pacing,
                enable_gso,
            ) {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    log::trace!("send() would block");
                    break;
                }

                panic!("send_to() failed: {:?}", e);
            }

            log::trace!("{} written {} bytes", client.conn.trace_id(), total_write);

            if total_write >= max_send_burst {
                log::trace!("{} pause writing", client.conn.trace_id(),);
                continue_write = true;
                break;
            }
        }

        // Garbage collect closed connections.
        clients.retain(|_, ref mut c| {
            log::trace!("Collecting garbage");

            if c.conn.is_closed() {
                log::info!(
                    "{} connection collected {:?} {:?}",
                    c.conn.trace_id(),
                    c.conn.stats(),
                    c.conn.path_stats().collect::<Vec<quiche::PathStats>>()
                );

                for id in c.conn.source_ids() {
                    let id_owned = id.clone().into_owned();
                    clients_ids.remove(&id_owned);
                }
            }

            !c.conn.is_closed()
        });
    }
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

/// Generate a new pair of Source Connection ID and reset token.
pub fn generate_cid_and_reset_token<T: SecureRandom>(
    rng: &T,
) -> (quiche::ConnectionId<'static>, u128) {
    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
    rng.fill(&mut scid).unwrap();
    let scid = scid.to_vec().into();
    let mut reset_token = [0; 16];
    rng.fill(&mut reset_token).unwrap();
    let reset_token = u128::from_be_bytes(reset_token);
    (scid, reset_token)
}

fn handle_path_events(client: &mut Client) {
    while let Some(qe) = client.conn.path_event_next() {
        match qe {
            quiche::PathEvent::New(local_addr, peer_addr) => {
                log::info!(
                    "{} Seen new path ({}, {})",
                    client.conn.trace_id(),
                    local_addr,
                    peer_addr
                );

                // Directly probe the new path.
                client
                    .conn
                    .probe_path(local_addr, peer_addr)
                    .expect("cannot probe");
            }

            quiche::PathEvent::Validated(local_addr, peer_addr) => {
                log::info!(
                    "{} Path ({}, {}) is now validated",
                    client.conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }

            quiche::PathEvent::FailedValidation(local_addr, peer_addr) => {
                log::info!(
                    "{} Path ({}, {}) failed validation",
                    client.conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }

            quiche::PathEvent::Closed(local_addr, peer_addr) => {
                log::info!(
                    "{} Path ({}, {}) is now closed and unusable",
                    client.conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }

            quiche::PathEvent::ReusedSourceConnectionId(cid_seq, old, new) => {
                log::info!(
                    "{} Peer reused cid seq {} (initially {:?}) on {:?}",
                    client.conn.trace_id(),
                    cid_seq,
                    old,
                    new
                );
            }

            quiche::PathEvent::PeerMigrated(local_addr, peer_addr) => {
                log::info!(
                    "{} Connection migrated to ({}, {})",
                    client.conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }
        }
    }
}

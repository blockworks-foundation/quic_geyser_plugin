use std::{collections::HashMap, net::SocketAddr};

use itertools::Itertools;
use mio::net::UdpSocket;
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
    pub conn: Connection,
    pub partial_responses: PartialResponses,
    pub read_streams: ReadStreams,
    pub filters: Vec<Filter>,
    pub next_stream: u64,
}

type ClientMap = HashMap<quiche::ConnectionId<'static>, Client>;

pub fn server_loop(
    mut config: quiche::Config,
    socket_addr: SocketAddr,
    mut message_send_queue: mio_channel::Receiver<ChannelMessage>,
    compression_type: CompressionType,
    stop_laggy_client: bool,
    maximum_concurrent_streams: u64,
) -> anyhow::Result<()> {
    let mut socket = UdpSocket::bind(socket_addr)?;

    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(1024);

    poll.registry().register(
        &mut socket,
        mio::Token(0),
        mio::Interest::READABLE | mio::Interest::WRITABLE,
    )?;

    poll.registry().register(
        &mut message_send_queue,
        mio::Token(1),
        mio::Interest::READABLE,
    )?;

    let local_addr = socket.local_addr()?;
    let rng = SystemRandom::new();
    let conn_id_seed = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap();
    let mut clients = ClientMap::new();
    loop {
        let timeout = clients.values().filter_map(|c| c.conn.timeout()).min();
        log::debug!("timeout : {}", timeout.unwrap_or_default().as_millis());

        poll.poll(&mut events, timeout).unwrap();

        let network_updates = true;
        let channel_updates = true;
        if network_updates {
            'read: loop {
                if events.is_empty() {
                    log::debug!("connection timed out");
                    clients.values_mut().for_each(|c| c.conn.on_timeout());
                    break 'read;
                }

                let (len, from) = match socket.recv_from(&mut buf) {
                    Ok(v) => v,
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            log::debug!("recv() would block");
                            break 'read;
                        }
                        panic!("recv() failed: {:?}", e);
                    }
                };

                log::debug!("got {} bytes", len);

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

                let conn_id = ring::hmac::sign(&conn_id_seed, &hdr.dcid);
                let conn_id = &conn_id.as_ref()[..quiche::MAX_CONN_ID_LEN];
                let conn_id: ConnectionId<'static> = conn_id.to_vec().into();
                let client = if !clients.contains_key(&hdr.dcid) && !clients.contains_key(&conn_id)
                {
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
                                log::debug!("send() would block");
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
                                log::debug!("send() would block");
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

                    log::debug!("New connection: dcid={:?} scid={:?}", hdr.dcid, scid);

                    let conn = quiche::accept(&scid, odcid.as_ref(), local_addr, from, &mut config)
                        .unwrap();

                    let client = Client {
                        conn,
                        partial_responses: PartialResponses::new(),
                        read_streams: ReadStreams::new(),
                        filters: Vec::new(),
                        next_stream: get_next_unidi(0, true, maximum_concurrent_streams),
                    };
                    clients.insert(scid.clone(), client);
                    clients
                        .get_mut(&scid)
                        .expect("should return last added client")
                } else {
                    // get the existing client
                    match clients.get_mut(&hdr.dcid) {
                        Some(v) => v,
                        None => clients
                            .get_mut(&conn_id)
                            .expect("The client should exist in the map"),
                    }
                };

                let recv_info = quiche::RecvInfo {
                    to: socket.local_addr().unwrap(),
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

                if client.conn.is_in_early_data() || client.conn.is_established() {
                    for stream_id in client.conn.writable() {
                        handle_writable(&mut client.conn, &mut client.partial_responses, stream_id);
                    }

                    // Process all readable streams.
                    for stream in client.conn.readable() {
                        let message =
                            recv_message(&mut client.conn, &mut client.read_streams, stream);
                        match message {
                            Ok(Some(message)) => match message {
                                Message::Filters(mut filters) => {
                                    client.filters.append(&mut filters);
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
            }

            if channel_updates {
                while let Ok(message) = message_send_queue.try_recv() {
                    let dispatch_to = clients
                        .iter_mut()
                        .filter(|(_, client)| {
                            client.conn.is_established()
                                && client.filters.iter().any(|x| x.allows(&message))
                        })
                        .map(|x| x.1)
                        .collect_vec();
                    if !dispatch_to.is_empty() {
                        let (message, priority) = match message {
                            ChannelMessage::Account(account, slot, _) => {
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
                            ChannelMessage::BlockMeta(block_meta) => {
                                (Message::BlockMetaMsg(block_meta), 2)
                            }
                            ChannelMessage::Transaction(transaction) => {
                                (Message::TransactionMsg(transaction), 3)
                            }
                        };
                        let binary = bincode::serialize(&message)
                            .expect("Message should be serializable in binary");
                        for client in dispatch_to {
                            let stream_id = client.next_stream;
                            match client.conn.stream_priority(stream_id, priority, true) {
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

                            client.next_stream =
                                get_next_unidi(stream_id, true, maximum_concurrent_streams);
                            log::debug!(
                                "dispatching {} on stream id : {}",
                                binary.len(),
                                stream_id
                            );
                            if let Err(e) = send_message(
                                &mut client.conn,
                                &mut client.partial_responses,
                                stream_id,
                                &binary,
                            ) {
                                log::error!("Error sending message : {e}");
                                if stop_laggy_client {
                                    log::info!(
                                        "Stopping laggy client : {}",
                                        client.conn.trace_id()
                                    );
                                    if let Err(e) = client.conn.close(true, 1, b"laggy client") {
                                        log::error!("error closing client : {}", e);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Generate outgoing QUIC packets for all active connections and send
            // them on the UDP socket, until quiche reports that there are no more
            // packets to be sent.
            for client in clients.values_mut() {
                loop {
                    let (write, send_info) = match client.conn.send(&mut out) {
                        Ok(v) => v,

                        Err(quiche::Error::Done) => {
                            log::debug!("{} done writing", client.conn.trace_id());
                            break;
                        }

                        Err(e) => {
                            log::error!("{} send failed: {:?}", client.conn.trace_id(), e);

                            client.conn.close(false, 0x1, b"fail").ok();
                            break;
                        }
                    };

                    if let Err(e) = socket.send_to(&out[..write], send_info.to) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            log::debug!("send() would block");
                            break;
                        }

                        log::error!("send() failed: {:?}", e);
                    }

                    log::debug!("{} written {} bytes", client.conn.trace_id(), write);
                }
            }

            // Garbage collect closed connections.
            clients.retain(|_, ref mut c| {
                log::debug!("Collecting garbage");

                if c.conn.is_closed() {
                    log::debug!(
                        "{} connection collected {:?}",
                        c.conn.trace_id(),
                        c.conn.stats()
                    );
                }

                !c.conn.is_closed()
            });
        }
    }
}

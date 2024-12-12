// Copyright (C) 2020, Cloudflare, Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
// IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
// PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
// EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
// PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
// PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
// LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

use crate::configure_server::configure_server;
use itertools::Itertools;
use log::trace;
use mio::Interest;
use mio::Token;
use prometheus::opts;
use prometheus::register_int_gauge;
use prometheus::IntGauge;
use quic_geyser_common::channel_message::ChannelMessage;
use quic_geyser_common::compression::CompressionType;
use quic_geyser_common::config::QuicParameters;
use quic_geyser_common::defaults::DEFAULT_PARALLEL_STREAMS;
use quic_geyser_common::defaults::MAX_DATAGRAM_SIZE;
use quic_geyser_common::filters::Filter;
use quic_geyser_common::message::Message;
use quic_geyser_common::types::account::Account;
use quic_geyser_common::types::block_meta::SlotMeta;
use quic_geyser_common::types::slot_identifier::SlotIdentifier;
use quic_geyser_quiche_utils::quiche_reciever::recv_message;
use quic_geyser_quiche_utils::quiche_reciever::ReadStreams;
use quic_geyser_quiche_utils::quiche_sender::handle_writable;
use quic_geyser_quiche_utils::quiche_sender::send_message;
use quic_geyser_quiche_utils::quiche_utils::detect_gso;
use quic_geyser_quiche_utils::quiche_utils::generate_cid_and_reset_token;
use quic_geyser_quiche_utils::quiche_utils::get_next_unidi;
use quic_geyser_quiche_utils::quiche_utils::handle_path_events;
use quic_geyser_quiche_utils::quiche_utils::mint_token;
use quic_geyser_quiche_utils::quiche_utils::send_with_pacing;
use quic_geyser_quiche_utils::quiche_utils::set_txtime_sockopt;
use quic_geyser_quiche_utils::quiche_utils::validate_token;
use quic_geyser_quiche_utils::quiche_utils::StreamBufferMap;
use quic_geyser_quiche_utils::quiche_utils::SEND_BUFFER_LEN;
use quiche::ConnectionId;
use ring::rand::*;
use std::collections::HashMap;
use std::net::SocketAddr;

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

pub type ClientId = u64;

pub struct Client {
    pub conn: quiche::Connection,
    pub client_id: ClientId,
    pub partial_requests: ReadStreams,
    pub partial_responses: StreamBufferMap<SEND_BUFFER_LEN>,
    pub max_datagram_size: usize,
    pub loss_rate: f64,
    pub max_send_burst: usize,
    pub connected: bool,
    pub closed: bool,
    pub filters: Vec<Filter>,
    pub next_stream: u64,
}

pub type ClientIdMap = HashMap<ConnectionId<'static>, ClientId>;
pub type ClientMap = HashMap<ClientId, Client>;

fn channel_message_to_message_priority(
    message: ChannelMessage,
    compression_type: CompressionType,
) -> (Message, u8) {
    match message {
        ChannelMessage::Account(account, slot, _init) => {
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
    }
}
pub fn server_loop(
    quic_params: QuicParameters,
    socket_addr: SocketAddr,
    mut message_send_queue: mio_channel::Receiver<ChannelMessage>,
    compression_type: CompressionType,
) -> anyhow::Result<()> {
    let mut config = configure_server(&quic_params)?;
    let incremental_priority = quic_params.incremental_priority;
    let stop_laggy_client = quic_params.disconnect_laggy_client;

    let mut buf = [0; 65535];

    // Setup the event loop.
    let mut poll = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(1024);

    // Create the UDP listening socket, and register it with the event loop.
    let mut socket = mio::net::UdpSocket::bind(socket_addr).unwrap();

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

    let rng = SystemRandom::new();
    let conn_id_seed = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap();

    let mut next_client_id = 0;
    let mut clients_ids = ClientIdMap::new();
    let mut clients = ClientMap::new();

    let mut continue_write = false;

    let local_addr = socket.local_addr().unwrap();
    let first_stream = get_next_unidi(3, true, u64::MAX);
    let mut message_queue_unregistered = false;

    loop {
        // Find the shorter timeout from all the active connections.
        //
        // TODO: use event loop that properly supports timers
        let timeout = match continue_write {
            true => Some(std::time::Duration::from_secs(0)),
            false => clients.values().filter_map(|c| c.conn.timeout()).min(),
        };

        let mut poll_res = poll.poll(&mut events, timeout);
        while let Err(e) = poll_res.as_ref() {
            if e.kind() == std::io::ErrorKind::Interrupted {
                log::trace!("mio poll() call failed, retrying: {:?}", e);
                poll_res = poll.poll(&mut events, timeout);
            } else {
                panic!("mio poll() call failed fatally: {:?}", e);
            }
        }

        if events.iter().any(|x| x.token() == Token(1)) {
            if clients.is_empty() {
                // no clients, no need to process messages
                while message_send_queue.try_recv().is_ok() {
                    // do nothing / clearing the queue
                }
                continue;
            }
            // check if streams are already full, avoid depiling messages if it is full
            // if stop_laggy_client is true then the client will be disconnected at any sign of lag
            // if stp_laggy_client is false then the client will not be disconnected and there are two scenarios
            // 1. if there are multiple clients and one of the client is laggy then the message will be dropped for laggy client
            // 2. all clients are laggy or there is one laggy client messages will be paused till the client gets some buffer space
            if stop_laggy_client
                || !clients.iter().any(|x| {
                    if x.1.partial_responses.is_empty() {
                        false
                    } else {
                        x.1.partial_responses.iter().all(|(_, y)| y.is_near_full())
                    }
                })
            {
                // dispactch messages to appropriate queues
                while let Ok(message) = message_send_queue.try_recv() {
                    let dispatching_connections = clients
                        .iter_mut()
                        .filter_map(|(_id, x)| {
                            if !x.connected || x.closed {
                                None
                            } else if x.filters.iter().any(|x| x.allows(&message)) {
                                Some(x)
                            } else {
                                None
                            }
                        })
                        .collect_vec();

                    if !dispatching_connections.is_empty() {
                        let (message, priority) =
                            channel_message_to_message_priority(message, compression_type);
                        let binary = message.to_binary_stream();
                        for client in dispatching_connections {
                            log::debug!("sending message to {}", client.client_id);
                            let stream_id = if client.partial_responses.len()
                                < DEFAULT_PARALLEL_STREAMS
                            {
                                let stream_id_to_use = client.next_stream;
                                client.next_stream =
                                    get_next_unidi(stream_id_to_use, true, u64::MAX);
                                log::debug!("Creating new stream to use :{stream_id_to_use}");
                                if stream_id_to_use == first_stream {
                                    // set high priority to first stream
                                    client
                                        .conn
                                        .stream_priority(stream_id_to_use, 0, incremental_priority)
                                        .unwrap();
                                } else {
                                    client
                                        .conn
                                        .stream_priority(stream_id_to_use, 1, incremental_priority)
                                        .unwrap();
                                }
                                stream_id_to_use
                            } else {
                                // for high priority streams
                                let stream_id = if priority == 0 {
                                    first_stream
                                } else {
                                    let value = client
                                        .partial_responses
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
                                &mut client.conn,
                                &mut client.partial_responses,
                                stream_id,
                                binary.clone(),
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

                            if close && stop_laggy_client {
                                if let Err(e) = client.conn.close(true, 1, b"laggy client") {
                                    if e != quiche::Error::Done {
                                        log::error!("error closing client : {}", e);
                                    }
                                } else {
                                    log::info!(
                                        "Stopping laggy client : {}",
                                        client.conn.trace_id(),
                                    );
                                }
                                client.closed = true;
                                break;
                            }
                        }

                        // if all buffers of a client are full do not continue
                        if clients.iter().any(|x| {
                            if x.1.partial_responses.is_empty() {
                                false
                            } else {
                                x.1.partial_responses.iter().all(|x| x.1.is_near_full())
                            }
                        }) {
                            // one of the client is full, stop sending message
                            break;
                        }
                    }
                }
            } else {
                // unregister message queue till clients get some buffer space
                poll.registry().deregister(&mut message_send_queue).unwrap();
                message_queue_unregistered = true;
            }
        }

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
            NUMBER_OF_BYTES_READ.add(len as i64);

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
            let conn_id = {
                let conn_id = ring::hmac::sign(&conn_id_seed, &hdr.dcid);
                let conn_id = &conn_id.as_ref()[..quiche::MAX_CONN_ID_LEN];
                conn_id.to_vec().into()
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
                    let len = quiche::negotiate_version(&hdr.scid, &hdr.dcid, &mut buf).unwrap();

                    let buf = &buf[..len];

                    if let Err(e) = socket.send_to(buf, from) {
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

                // Token is always present in Initial packets.
                let token = hdr.token.as_ref().unwrap();

                // Do stateless retry if the client didn't send a token.
                if token.is_empty() {
                    log::debug!("Doing stateless retry");
                    let scid = quiche::ConnectionId::from_ref(&scid);
                    let new_token = mint_token(&hdr, &from);

                    let len = quiche::retry(
                        &hdr.scid,
                        &hdr.dcid,
                        &scid,
                        &new_token,
                        hdr.version,
                        &mut buf,
                    )
                    .unwrap();

                    let buf = &buf[..len];

                    if let Err(e) = socket.send_to(buf, from) {
                        if e.kind() == std::io::ErrorKind::WouldBlock {
                            log::trace!("send() would block");
                            break;
                        }
                        panic!("send() failed: {:?}", e);
                    }
                    continue 'read;
                }

                let odcid = validate_token(&from, token);

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

                let scid = quiche::ConnectionId::from_vec(scid.to_vec());

                log::debug!("New connection: dcid={:?} scid={:?}", hdr.dcid, scid);
                #[allow(unused_mut)]
                let mut conn =
                    quiche::accept(&scid, odcid.as_ref(), local_addr, from, &mut config).unwrap();

                let client_id = next_client_id;

                let client = Client {
                    conn,
                    client_id,
                    partial_requests: ReadStreams::new(),
                    partial_responses: StreamBufferMap::new(),
                    max_datagram_size: MAX_DATAGRAM_SIZE,
                    loss_rate: 0.0,
                    max_send_burst: MAX_DATAGRAM_SIZE * 10,
                    connected: false,
                    closed: false,
                    filters: vec![],
                    next_stream: first_stream,
                };
                NUMBER_OF_CLIENTS.inc();
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

            log::trace!("{} processed {} bytes", client.conn.trace_id(), read);

            if !client.connected && client.conn.is_established() {
                client.connected = true;
            }

            if client.conn.is_in_early_data() || client.conn.is_established() {
                // Process all readable streams.
                for stream in client.conn.readable() {
                    NUMBER_OF_READ_COUNT.inc();
                    let message =
                        recv_message(&mut client.conn, &mut client.partial_requests, stream);
                    match message {
                        Ok(Some(messages)) => {
                            for message in messages {
                                let message = bincode::deserialize::<Message>(&message)
                                    .expect("Should be a message");
                                match message {
                                    Message::Filters(mut f) => {
                                        client.filters.append(&mut f);
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
                            log::error!("Error recieving message : {e}");
                            // missed the message close the connection
                            let _ = client.conn.close(true, 0, b"recv error");
                            client.closed = true;
                        }
                    }
                }
            }

            if !client.conn.is_closed()
                && (client.conn.is_established() || client.conn.is_in_early_data())
            {
                // Update max_datagram_size after connection established.
                client.max_datagram_size = client.conn.max_send_udp_payload_size();

                for stream_id in client.conn.writable() {
                    NUMBER_OF_WRITE_COUNT.inc();
                    if let Err(e) =
                        handle_writable(&mut client.conn, &mut client.partial_responses, stream_id)
                    {
                        if e == quiche::Error::Done {
                            break;
                        }
                        if !client.closed {
                            log::error!("Error writing {e:?}");
                            let _ = client.conn.close(true, 1, b"stream stopped");
                            client.closed = true;
                            break;
                        }
                    }
                }
            }

            handle_path_events(&mut client.conn);

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
                    match client.conn.send(&mut buf[total_write..max_send_burst]) {
                        Ok(v) => v,

                        Err(quiche::Error::Done) => {
                            trace!("{} done writing", client.conn.trace_id());
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
                continue;
            }

            let send_result = if enable_pacing {
                send_with_pacing(
                    &socket,
                    &buf[..total_write],
                    &dst_info.unwrap(),
                    enable_gso,
                    client.max_datagram_size as u16,
                )
            } else {
                socket.send(&buf[..total_write])
            };

            match send_result {
                Ok(written) => {
                    NUMBER_OF_BYTES_SENT.add(written as i64);
                    log::debug!("finished sending");
                    // check if any buffer has more than 75% space to restart recieving messages
                    if message_queue_unregistered
                        && written > 0
                        && client
                            .partial_responses
                            .iter()
                            .any(|x| x.1.has_more_than_required_capacity())
                    {
                        // wait for the message queue to have some space
                        poll.registry()
                            .register(&mut message_send_queue, Token(1), Interest::READABLE)
                            .unwrap();
                        message_queue_unregistered = false;
                    }
                }
                Err(e) => {
                    log::error!("sending failed with error : {e:?}");
                }
            }

            trace!("{} written {} bytes", client.conn.trace_id(), total_write);

            if total_write >= max_send_burst {
                trace!("{} pause writing", client.conn.trace_id(),);
                continue_write = true;
                break;
            }

            if client.conn.is_closed() {
                NUMBER_OF_CLIENTS.dec();
                if let Some(e) = client.conn.peer_error() {
                    log::error!("peer error : {e:?} ");
                }

                if let Some(e) = client.conn.local_error() {
                    log::error!("local error : {e:?} ");
                }

                log::info!(
                    "{} connection closed {:?}",
                    client.conn.trace_id(),
                    client.conn.stats()
                );
                break;
            }
        }

        // Garbage collect closed connections.
        clients.retain(|_, ref mut c| {
            trace!("Collecting garbage");

            if c.conn.is_closed() {
                NUMBER_OF_CONNECTION_CLOSED.inc();
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

                if message_queue_unregistered {
                    poll.registry()
                        .register(&mut message_send_queue, Token(1), Interest::READABLE)
                        .unwrap();
                    message_queue_unregistered = false;
                }
                false
            } else {
                true
            }
        });
    }
}

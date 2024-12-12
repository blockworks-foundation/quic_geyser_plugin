use std::{
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
};

use log::{debug, error, info, trace};
use quic_geyser_common::{
    defaults::MAX_DATAGRAM_SIZE, message::Message,
    types::connections_parameters::ConnectionParameters,
};

use quic_geyser_quiche_utils::{
    quiche_reciever::{recv_message, ReadStreams},
    quiche_sender::{handle_writable, send_message},
    quiche_utils::{
        detect_gso, generate_cid_and_reset_token, get_next_unidi, send_with_pacing,
        set_txtime_sockopt, StreamBufferMap,
    },
};

use anyhow::bail;
use ring::rand::{SecureRandom, SystemRandom};

use crate::configure_client::configure_client;

pub fn client_loop(
    connection_parameters: ConnectionParameters,
    socket_addr: SocketAddr,
    server_address: SocketAddr,
    mut message_send_queue: mio_channel::Receiver<Message>,
    message_recv_queue: std::sync::mpsc::Sender<Message>,
    is_connected: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let mut socket = mio::net::UdpSocket::bind(socket_addr)?;

    let mut config = configure_client(&connection_parameters)?;

    let enable_pacing = if connection_parameters.enable_pacing {
        set_txtime_sockopt(&socket).is_ok()
    } else {
        false
    };

    let enable_gso = if connection_parameters.enable_gso {
        detect_gso(&socket, MAX_DATAGRAM_SIZE)
    } else {
        false
    };

    let (message_binary_channel_sx, message_binary_channel_rx) =
        std::sync::mpsc::channel::<Vec<u8>>();
    let _message_deserializing_task = std::thread::spawn(move || loop {
        match message_binary_channel_rx.recv() {
            Ok(message_binary) => match bincode::deserialize::<Message>(&message_binary) {
                Ok(message) => {
                    if let Err(e) = message_recv_queue.send(message) {
                        log::error!("Error sending message on the channel : {e}");
                        break;
                    }
                }
                Err(e) => {
                    log::error!("Error deserializing message : {e:?}");
                }
            },
            Err(e) => {
                log::error!("recv failed: {:?}", e);
                break;
            }
        }
    });

    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(1024);
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];
    let mut read_streams = ReadStreams::new();
    const READ_BUFFER_SIZE: usize = 16 * 1024 * 1024; // 16 MBs
                                                      // client always sends on same stream
    let send_stream_id = get_next_unidi(4, false, u64::MAX);
    let mut has_connected = false;
    // Generate a random source connection ID for the connection.
    let rng = SystemRandom::new();

    let mut stream_sender_map = StreamBufferMap::<READ_BUFFER_SIZE>::new();

    poll.registry()
        .register(&mut socket, mio::Token(0), mio::Interest::READABLE)?;

    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
    if SystemRandom::new().fill(&mut scid[..]).is_err() {
        bail!("Error filling scid");
    }
    log::info!("connecing client with quiche");

    let scid = quiche::ConnectionId::from_ref(&scid);
    let local_addr = socket.local_addr()?;

    let mut conn = quiche::connect(
        Some("quiche_plugin_server"),
        &scid,
        local_addr,
        server_address,
        &mut config,
    )?;

    // sending initial connection request
    {
        let (write, send_info) = conn.send(&mut out).expect("initial send failed");

        if let Err(e) = socket.send_to(&out[..write], send_info.to) {
            bail!("send() failed: {:?}", e);
        }
    }

    let mut instance = Instant::now();
    let mut connection_recently_established = false;
    let ping_message = Message::Ping.to_binary_stream();
    let mut loss_rate = 0.0;
    let mut max_send_burst = MAX_DATAGRAM_SIZE * 10;
    let mut continue_write = true;
    let max_datagram_size = MAX_DATAGRAM_SIZE;
    loop {
        let timeout = match continue_write {
            true => Some(std::time::Duration::from_secs(0)),
            false => conn.timeout(),
        };

        poll.poll(&mut events, timeout).unwrap();

        if connection_recently_established || events.iter().any(|e| e.token() == mio::Token(1)) {
            if connection_recently_established {
                connection_recently_established = false;
            }
            match message_send_queue.try_recv() {
                Ok(message) => {
                    let binary_message = message.to_binary_stream();
                    log::info!("send message : {message:?}");
                    if let Err(e) = send_message(
                        &mut conn,
                        &mut stream_sender_map,
                        send_stream_id,
                        binary_message,
                    ) {
                        log::error!(
                            "Error sending filters : {e}, probably because filter is too long"
                        );
                    }
                }
                Err(e) => {
                    match e {
                        std::sync::mpsc::TryRecvError::Empty => {
                            // do nothing
                        }
                        std::sync::mpsc::TryRecvError::Disconnected => {
                            log::error!("recv failed: {:?}", e);
                            break;
                        }
                    }
                }
            }
        }

        // Read incoming UDP packets from the socket and feed them to quiche,
        // until there are no more packets to read.
        'read: loop {
            // If the event loop reported no events, it means that the timeout
            // has expired, so handle it without attempting to read packets. We
            // will then proceed with the send loop.
            if events.is_empty() {
                conn.on_timeout();
                break 'read;
            }

            let (len, from) = match socket.recv_from(&mut buf) {
                Ok(v) => v,

                Err(e) => {
                    // There are no more UDP packets to read, so end the read
                    // loop.
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        debug!("recv() would block");
                        break 'read;
                    }

                    log::error!("recv() failed: {:?}", e);
                    break;
                }
            };

            trace!("got {} bytes", len);

            let recv_info = quiche::RecvInfo {
                to: socket.local_addr().unwrap(),
                from,
            };

            // Process potentially coalesced packets.
            let read = match conn.recv(&mut buf[..len], recv_info) {
                Ok(v) => v,

                Err(e) => {
                    error!("recv failed: {:?}", e);
                    continue 'read;
                }
            };

            debug!("processed {} bytes", read);
        }

        debug!("done reading");

        if instance.elapsed() > Duration::from_secs(1) {
            log::debug!("sending ping to the server");
            if let Err(e) = send_message(
                &mut conn,
                &mut stream_sender_map,
                send_stream_id,
                ping_message.clone(),
            ) {
                log::error!("Error sending ping message : {e}");
            }
            instance = Instant::now();
        }

        if !has_connected && conn.is_established() {
            log::info!("connection established");
            has_connected = true;
            connection_recently_established = true;
            is_connected.store(true, std::sync::atomic::Ordering::Relaxed);
            poll.registry()
                .register(
                    &mut message_send_queue,
                    mio::Token(1),
                    mio::Interest::READABLE,
                )
                .unwrap();
        }
        // See whether source Connection IDs have been retired.
        while let Some(retired_scid) = conn.retired_scid_next() {
            log::info!("Retiring source CID {:?}", retired_scid);
        }

        // Provides as many CIDs as possible.
        while conn.scids_left() > 0 {
            let (scid, reset_token) = generate_cid_and_reset_token(&rng);

            if conn.new_scid(&scid, reset_token, false).is_err() {
                break;
            }
        }

        if conn.is_established() {
            // Process all readable streams.
            for s in conn.readable() {
                let message = recv_message(&mut conn, &mut read_streams, s);
                match message {
                    Ok(Some(messages)) => {
                        log::debug!("got messages: {}", messages.len());
                        for message in messages {
                            if let Err(e) = message_binary_channel_sx.send(message) {
                                log::error!("Error sending message on the channel : {e}");
                                break;
                            }
                        }
                    }
                    Ok(None) => {
                        // do nothing / continue
                    }
                    Err(e) => {
                        log::error!("Error recieving message : {e}");
                        let _ = conn.close(true, 1, b"error recieving");
                    }
                }
            }

            for s in conn.writable() {
                if let Err(e) = handle_writable(&mut conn, &mut stream_sender_map, s) {
                    log::error!("Error handling writable stream : {e:?}");
                }
            }
        }

        // Reduce max_send_burst by 25% if loss is increasing more than 0.1%.
        let calculated_loss_rate = conn.stats().lost as f64 / conn.stats().sent as f64;
        if calculated_loss_rate > loss_rate + 0.001 {
            max_send_burst = max_send_burst / 4 * 3;
            // Minimun bound of 10xMSS.
            max_send_burst = max_send_burst.max(max_datagram_size * 10);
            loss_rate = calculated_loss_rate;
        }

        let max_send_burst =
            conn.send_quantum().min(max_send_burst) / max_datagram_size * max_datagram_size;
        let mut total_write = 0;
        let mut dst_info = None;

        while total_write < max_send_burst {
            let (write, mut send_info) = match conn.send(&mut buf[total_write..max_send_burst]) {
                Ok(v) => v,
                Err(quiche::Error::Done) => {
                    trace!("{} done writing", conn.trace_id());
                    break;
                }
                Err(e) => {
                    log::error!("{} send failed: {:?}", conn.trace_id(), e);
                    conn.close(false, 0x1, b"fail").ok();
                    break;
                }
            };

            total_write += write;

            // Use the first packet time to send, not the last.
            send_info.to = server_address;
            let _ = dst_info.get_or_insert(send_info);

            if write < max_datagram_size {
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
                max_datagram_size as u16,
            )
        } else {
            socket.send(&buf[..total_write])
        };

        if let Err(e) = send_result {
            log::error!("sending failed with error : {e:?}");
            break;
        }

        if conn.is_closed() {
            info!("connection closed, {:?}", conn.stats());
            is_connected.store(false, std::sync::atomic::Ordering::Relaxed);
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv6Addr, SocketAddr},
        sync::{atomic::AtomicBool, mpsc, Arc},
        thread::sleep,
        time::Duration,
    };

    use itertools::Itertools;
    use quic_geyser_server::quiche_server_loop::server_loop;
    use solana_sdk::{account::Account, pubkey::Pubkey};

    use quic_geyser_common::{
        channel_message::{AccountData, ChannelMessage},
        compression::CompressionType,
        config::QuicParameters,
        filters::Filter,
        message::Message,
        net::parse_host_port,
        types::{block_meta::SlotMeta, connections_parameters::ConnectionParameters},
    };

    use super::client_loop;

    #[test]
    fn test_send_and_recieve_of_large_account_with_client_loop() {
        // Setup the event loop.
        let socket_addr = parse_host_port("[::]:10900").unwrap();

        let port = 10900;
        let maximum_concurrent_streams = 100;

        let message_1 = ChannelMessage::Slot(
            3,
            2,
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        );
        let message_2 = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: Account {
                    lamports: 12345,
                    data: (0..100).map(|_| rand::random::<u8>()).collect_vec(),
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version: 1,
            },
            5,
            false,
        );

        let message_3 = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: Account {
                    lamports: 23456,
                    data: (0..10_000).map(|_| rand::random::<u8>()).collect_vec(),
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version: 1,
            },
            5,
            false,
        );

        let message_4 = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: Account {
                    lamports: 34567,
                    data: (0..1_000_000).map(|_| rand::random::<u8>()).collect_vec(),
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version: 1,
            },
            5,
            false,
        );

        let message_5 = ChannelMessage::Account(
            AccountData {
                pubkey: Pubkey::new_unique(),
                account: Account {
                    lamports: 45678,
                    data: (0..10_000_000).map(|_| rand::random::<u8>()).collect_vec(),
                    owner: Pubkey::new_unique(),
                    executable: false,
                    rent_epoch: u64::MAX,
                },
                write_version: 1,
            },
            5,
            false,
        );

        // server loop
        let (server_send_queue, rx_sent_queue) = mio_channel::channel::<ChannelMessage>();
        let _server_loop_jh = std::thread::spawn(move || {
            if let Err(e) = server_loop(
                QuicParameters {
                    incremental_priority: true,
                    ..Default::default()
                },
                socket_addr,
                rx_sent_queue,
                CompressionType::Lz4Fast(8),
            ) {
                log::error!("Server loop closed by error : {e}");
            }
        });

        // client loop
        let server_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port);
        let (client_sx_queue, rx_sent_queue) = mio_channel::channel();
        let (sx_recv_queue, client_rx_queue) = mpsc::channel();

        let _client_loop_jh = std::thread::spawn(move || {
            let client_config = ConnectionParameters {
                max_number_of_streams: maximum_concurrent_streams,
                ..Default::default()
            };
            let socket_addr: SocketAddr = parse_host_port("[::]:0").unwrap();
            let is_connected = Arc::new(AtomicBool::new(false));
            if let Err(e) = client_loop(
                client_config,
                socket_addr,
                server_addr,
                rx_sent_queue,
                sx_recv_queue,
                is_connected,
            ) {
                log::error!("client stopped with error {e}");
            }
        });
        client_sx_queue
            .send(Message::Filters(vec![
                Filter::AccountsAll,
                Filter::TransactionsAll,
                Filter::Slot,
            ]))
            .unwrap();
        sleep(Duration::from_millis(100));
        server_send_queue.send(message_1.clone()).unwrap();
        server_send_queue.send(message_2.clone()).unwrap();
        server_send_queue.send(message_3.clone()).unwrap();
        sleep(Duration::from_millis(100));
        server_send_queue.send(message_4.clone()).unwrap();
        server_send_queue.send(message_5.clone()).unwrap();
        sleep(Duration::from_millis(100));

        let message_rx_1 = client_rx_queue.recv().unwrap();
        assert_eq!(
            message_rx_1,
            Message::SlotMsg(SlotMeta {
                slot: 3,
                parent: 2,
                commitment_config: solana_sdk::commitment_config::CommitmentConfig::confirmed()
            })
        );

        let message_rx_2 = client_rx_queue.recv().unwrap();

        let ChannelMessage::Account(account, slot, _) = &message_2 else {
            panic!("message should be account");
        };
        let Message::AccountMsg(message_rx_2) = message_rx_2 else {
            panic!("message should be account");
        };
        let message_account = message_rx_2.solana_account();
        assert_eq!(account.pubkey, message_rx_2.pubkey);
        assert_eq!(account.account, message_account);
        assert_eq!(message_rx_2.slot_identifier.slot, *slot);

        let message_rx_3 = client_rx_queue.recv().unwrap();

        let ChannelMessage::Account(account, slot, _) = &message_3 else {
            panic!("message should be account");
        };
        let Message::AccountMsg(message_rx_3) = message_rx_3 else {
            panic!("message should be account");
        };
        let message_account = message_rx_3.solana_account();
        assert_eq!(account.pubkey, message_rx_3.pubkey);
        assert_eq!(account.account, message_account);
        assert_eq!(message_rx_3.slot_identifier.slot, *slot);

        let message_rx_4 = client_rx_queue.recv().unwrap();
        let ChannelMessage::Account(account, slot, _) = &message_4 else {
            panic!("message should be account");
        };
        let Message::AccountMsg(message_rx_4) = message_rx_4 else {
            panic!("message should be account");
        };
        let message_account = message_rx_4.solana_account();
        assert_eq!(account.pubkey, message_rx_4.pubkey);
        assert_eq!(account.account, message_account);
        assert_eq!(message_rx_4.slot_identifier.slot, *slot);

        let message_rx_5 = client_rx_queue.recv().unwrap();
        let ChannelMessage::Account(account, slot, _) = &message_5 else {
            panic!("message should be account");
        };
        let Message::AccountMsg(message_rx_5) = message_rx_5 else {
            panic!("message should be account");
        };
        let message_account = message_rx_5.solana_account();
        assert_eq!(account.pubkey, message_rx_5.pubkey);
        assert_eq!(account.account, message_account);
        assert_eq!(message_rx_5.slot_identifier.slot, *slot);
    }
}

use std::{
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
    time::{Duration, Instant},
    u64,
};

use quic_geyser_common::{
    defaults::{MAX_DATAGRAM_SIZE, MAX_PAYLOAD_BUFFER},
    message::Message,
};

use quic_geyser_quiche_utils::{
    quiche_reciever::{recv_message, ReadStreams},
    quiche_sender::{handle_writable, send_message},
    quiche_utils::{generate_cid_and_reset_token, get_next_unidi, StreamSenderMap},
};

use anyhow::bail;
use ring::rand::{SecureRandom, SystemRandom};

pub fn client_loop(
    mut config: quiche::Config,
    socket_addr: SocketAddr,
    server_address: SocketAddr,
    message_send_queue: std::sync::mpsc::Receiver<Message>,
    message_recv_queue: std::sync::mpsc::Sender<Message>,
    is_connected: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let mut socket = mio::net::UdpSocket::bind(socket_addr)?;
    let mut poll = mio::Poll::new()?;
    let mut events = mio::Events::with_capacity(1024);

    poll.registry()
        .register(&mut socket, mio::Token(0), mio::Interest::READABLE)?;

    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
    if SystemRandom::new().fill(&mut scid[..]).is_err() {
        bail!("Error filling scid");
    }
    log::info!("connecing client with quiche");

    let scid = quiche::ConnectionId::from_ref(&scid);
    let local_addr = socket.local_addr()?;

    let mut connection = quiche::connect(None, &scid, local_addr, server_address, &mut config)?;

    // sending initial connection request
    {
        let mut out = [0; MAX_DATAGRAM_SIZE];
        let (write, send_info) = connection.send(&mut out).expect("initial send failed");

        if let Err(e) = socket.send_to(&out[..write], send_info.to) {
            bail!("send() failed: {:?}", e);
        }
    }

    let (data_to_quiche, quiche_data_receiver) = mio_channel::channel();
    let (quiche_data_sender, mut data_from_quiche) = mio_channel::channel();

    poll.registry()
        .register(
            &mut data_from_quiche,
            mio::Token(1),
            mio::Interest::READABLE,
        )
        .unwrap();

    create_quiche_client_thread(
        connection,
        quiche_data_receiver,
        quiche_data_sender,
        message_send_queue,
        message_recv_queue,
        is_connected,
    );

    let mut buf = [0; 65535];
    'client: loop {
        poll.poll(&mut events, Some(Duration::from_millis(10)))?;

        'read: loop {
            match socket.recv_from(&mut buf) {
                Ok((len, from)) => {
                    let recv_info = quiche::RecvInfo {
                        to: socket.local_addr()?,
                        from,
                    };
                    if data_to_quiche
                        .send((recv_info, buf[..len].to_vec()))
                        .is_err()
                    {
                        // client is closed
                        break 'client;
                    }
                }
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        break 'read;
                    }
                    bail!("recv() failed: {:?}", e);
                }
            };
        }

        // Generate outgoing QUIC packets and send them on the UDP socket, until
        // quiche reports that there are no more packets to be sent.
        while let Ok((send_info, buf)) = data_from_quiche.try_recv() {
            match socket.send_to(&buf, send_info.to) {
                Ok(_len) => {}
                Err(e) => {
                    if e.kind() == std::io::ErrorKind::WouldBlock {
                        log::debug!("send() would block");
                        break;
                    }
                    log::error!("send() failed: {:?}", e);
                }
            }
        }
    }
    Ok(())
}

pub fn create_quiche_client_thread(
    connection: quiche::Connection,
    mut receiver: mio_channel::Receiver<(quiche::RecvInfo, Vec<u8>)>,
    sender: mio_channel::Sender<(quiche::SendInfo, Vec<u8>)>,
    message_send_queue: std::sync::mpsc::Receiver<Message>,
    message_recv_queue: std::sync::mpsc::Sender<Message>,
    is_connected: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let mut connection = connection;

        let mut poll = mio::Poll::new().unwrap();
        let mut events = mio::Events::with_capacity(1024);

        poll.registry()
            .register(&mut receiver, mio::Token(1), mio::Interest::READABLE)
            .unwrap();
        let stream_id = get_next_unidi(0, false, u64::MAX);
        let mut stream_sender_map = StreamSenderMap::new();
        let mut read_streams = ReadStreams::new();
        let mut connected = false;
        let mut instance = Instant::now();
        // Generate a random source connection ID for the connection.
        let rng = SystemRandom::new();

        'client: loop {
            poll.poll(&mut events, Some(Duration::from_millis(10)))
                .unwrap();
            if events.is_empty() {
                connection.on_timeout();
            }

            if instance.elapsed() > Duration::from_secs(1) {
                log::debug!("sending ping to the server");
                if let Err(e) = send_message(
                    &mut connection,
                    &mut stream_sender_map,
                    stream_id,
                    Message::Ping.to_binary_stream(),
                ) {
                    log::error!("Error sending ping message : {e}");
                }
                instance = Instant::now();
                connection.on_timeout();
            }

            while let Ok((recv_info, mut buf)) = receiver.try_recv() {
                // Process potentially coalesced packets.
                if let Err(e) = connection.recv(buf.as_mut_slice(), recv_info) {
                    match e {
                        quiche::Error::Done => {
                            // done reading
                            break;
                        }
                        _ => {
                            log::error!("recv failed: {:?}", e);
                            break;
                        }
                    }
                };
            }

            if !connected && connection.is_established() {
                is_connected.store(true, std::sync::atomic::Ordering::Relaxed);
                connected = true;
            }

            // See whether source Connection IDs have been retired.
            while let Some(retired_scid) = connection.retired_scid_next() {
                log::info!("Retiring source CID {:?}", retired_scid);
            }

            // Provides as many CIDs as possible.
            while connection.scids_left() > 0 {
                let (scid, reset_token) = generate_cid_and_reset_token(&rng);

                if connection.new_scid(&scid, reset_token, false).is_err() {
                    break;
                }
            }

            // chanel updates
            if connection.is_established() {
                // io events
                for stream_id in connection.readable() {
                    log::debug!("got readable stream");
                    let message = recv_message(&mut connection, &mut read_streams, stream_id);
                    match message {
                        Ok(Some(messages)) => {
                            log::debug!("got messages: {}", messages.len());
                            for message in messages {
                                if let Err(e) = message_recv_queue.send(message) {
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
                            let _ = connection.close(true, 1, b"error recieving");
                        }
                    }
                }

                loop {
                    match message_send_queue.try_recv() {
                        Ok(message_to_send) => {
                            log::debug!(
                                "sending message: {message_to_send:?} on stream : {stream_id:?}"
                            );
                            let binary = message_to_send.to_binary_stream();
                            log::debug!("finished binary message of length {}", binary.len());
                            if let Err(e) = send_message(
                                &mut connection,
                                &mut stream_sender_map,
                                stream_id,
                                binary,
                            ) {
                                log::error!("Sending failed with error {e:?}");
                            }
                        }
                        Err(e) => {
                            match e {
                                std::sync::mpsc::TryRecvError::Empty => {
                                    // no more new messages
                                }
                                std::sync::mpsc::TryRecvError::Disconnected => {
                                    let _ = connection.close(true, 0, b"no longer needed");
                                }
                            }
                            break;
                        }
                    }
                }
            }

            for stream_id in connection.writable() {
                if let Err(e) = handle_writable(&mut connection, &mut stream_sender_map, stream_id)
                {
                    if e != quiche::Error::Done {
                        log::error!("Error writing message on writable stream : {e:?}");
                    }
                }
            }

            if connection.is_closed() {
                is_connected.store(false, std::sync::atomic::Ordering::Relaxed);
                log::info!("connection closed, {:?}", connection.stats());
                break;
            }

            let mut out = vec![0; MAX_PAYLOAD_BUFFER];
            loop {
                match connection.send(&mut out) {
                    Ok((write, send_info)) => {
                        log::debug!("sending :{}", write);
                        if sender.send((send_info, out[..write].to_vec())).is_err() {
                            log::error!("client socket thread broken");
                            break 'client;
                        }
                    }
                    Err(quiche::Error::Done) => {
                        break;
                    }
                    Err(e) => {
                        log::error!("send failed: {:?}", e);
                        connection.close(false, 0x1, b"fail").ok();
                        break;
                    }
                };
            }
        }
    });
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
        types::block_meta::SlotMeta,
    };

    use crate::configure_client::configure_client;

    use super::client_loop;

    #[test]
    fn test_send_and_recieve_of_large_account_with_client_loop() {
        tracing_subscriber::fmt::init();
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
        let (server_send_queue, rx_sent_queue) = mpsc::channel::<ChannelMessage>();
        let _server_loop_jh = std::thread::spawn(move || {
            if let Err(e) = server_loop(
                QuicParameters {
                    incremental_priority: true,
                    ..Default::default()
                },
                socket_addr,
                rx_sent_queue,
                CompressionType::Lz4Fast(8),
                true,
            ) {
                log::error!("Server loop closed by error : {e}");
            }
        });

        // client loop
        let server_addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), port);
        let (client_sx_queue, rx_sent_queue) = mpsc::channel();
        let (sx_recv_queue, client_rx_queue) = mpsc::channel();

        let _client_loop_jh = std::thread::spawn(move || {
            let client_config =
                configure_client(maximum_concurrent_streams, 20_000_000, 1, 25, 3).unwrap();
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

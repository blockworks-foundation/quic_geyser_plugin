use std::{
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    message::Message,
    quic::{
        configure_server::MAX_DATAGRAM_SIZE,
        quiche_reciever::{recv_message, ReadStreams},
        quiche_sender::{handle_writable, send_message},
        quiche_utils::{get_next_unidi, PartialResponses},
    },
};
use anyhow::bail;
use ring::rand::{SecureRandom, SystemRandom};

pub fn client_loop(
    mut config: quiche::Config,
    socket_addr: SocketAddr,
    server_address: SocketAddr,
    mut message_send_queue: mio_channel::Receiver<Message>,
    message_recv_queue: std::sync::mpsc::Sender<Message>,
    is_connected: Arc<AtomicBool>,
    maximum_streams: u64,
) -> anyhow::Result<()> {
    let mut socket = mio::net::UdpSocket::bind(socket_addr)?;
    let mut poll = mio::Poll::new()?;

    let mut events = mio::Events::with_capacity(1024);

    poll.registry()
        .register(&mut socket, mio::Token(0), mio::Interest::READABLE)?;

    poll.registry().register(
        &mut message_send_queue,
        mio::Token(1),
        mio::Interest::READABLE,
    )?;

    let mut scid = [0; quiche::MAX_CONN_ID_LEN];
    if SystemRandom::new().fill(&mut scid[..]).is_err() {
        bail!("Error filling scid");
    }
    log::info!("connecing client with quiche");

    let scid = quiche::ConnectionId::from_ref(&scid);
    let local_addr = socket.local_addr().unwrap();

    let mut conn = quiche::connect(None, &scid, local_addr, server_address, &mut config)?;

    let mut out = [0; MAX_DATAGRAM_SIZE];
    let (write, send_info) = conn.send(&mut out).expect("initial send failed");

    if let Err(e) = socket.send_to(&out[..write], send_info.to) {
        bail!("send() failed: {:?}", e);
    }

    let mut current_stream_id = 3;
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];
    let mut partial_responses = PartialResponses::new();
    let mut read_streams = ReadStreams::new();
    let mut connected = false;

    'client: loop {
        poll.poll(&mut events, conn.timeout()).unwrap();
        if events.is_empty() {
            log::debug!("connection timed out");
            conn.on_timeout();
            bail!("connection timed out");
        }

        if conn.is_closed() {
            log::info!("connection closed, {:?}", conn.stats());
            break;
        }

        let network_updates = true;
        let channel_updates = true;

        if network_updates {
            'read: loop {
                if events.is_empty() {
                    log::debug!("timed out");

                    conn.on_timeout();
                    break 'client;
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

                let recv_info = quiche::RecvInfo {
                    to: socket.local_addr().unwrap(),
                    from,
                };

                // Process potentially coalesced packets.
                let read = match conn.recv(&mut buf[..len], recv_info) {
                    Ok(v) => v,

                    Err(e) => {
                        log::error!("recv failed: {:?}", e);
                        continue 'read;
                    }
                };

                log::debug!("processed {} bytes", read);
            }

            // io events
            for stream_id in conn.readable() {
                let message = recv_message(&mut conn, &mut read_streams, stream_id);
                match message {
                    Ok(Some(message)) => {
                        message_recv_queue.send(message).unwrap();
                    }
                    Ok(None) => {
                        // do nothing
                    }
                    Err(e) => {
                        log::error!("Error recieving message : {e}")
                    }
                }
            }

            for stream_id in conn.writable() {
                handle_writable(&mut conn, &mut partial_responses, stream_id);
            }
        }

        if !connected && conn.is_established() {
            is_connected.store(true, std::sync::atomic::Ordering::Relaxed);
            connected = true;
        }

        // chanel updates
        if channel_updates && conn.is_established() {
            // channel events
            if let Ok(message_to_send) = message_send_queue.try_recv() {
                current_stream_id = get_next_unidi(current_stream_id, false, maximum_streams);
                let binary =
                    bincode::serialize(&message_to_send).expect("Message should be serializable");
                if let Err(e) = send_message(
                    &mut conn,
                    &mut partial_responses,
                    current_stream_id,
                    &binary,
                ) {
                    log::error!("Sending failed with error {e:?}");
                }
            }
        }

        // Generate outgoing QUIC packets and send them on the UDP socket, until
        // quiche reports that there are no more packets to be sent.
        loop {
            let (write, send_info) = match conn.send(&mut out) {
                Ok(v) => v,

                Err(quiche::Error::Done) => {
                    log::debug!("done writing");
                    break;
                }

                Err(e) => {
                    log::error!("send failed: {:?}", e);

                    conn.close(false, 0x1, b"fail").ok();
                    bail!("writing failed");
                }
            };

            if let Err(e) = socket.send_to(&out[..write], send_info.to) {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    log::debug!("send() would block");
                    break;
                }

                bail!("send fail");
            }
            log::debug!("written {}", write);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        str::FromStr,
        sync::{atomic::AtomicBool, mpsc, Arc},
        thread::sleep,
        time::Duration,
    };

    use itertools::Itertools;
    use solana_sdk::{account::Account, pubkey::Pubkey};

    use crate::{
        channel_message::{AccountData, ChannelMessage},
        compression::CompressionType,
        filters::Filter,
        message::Message,
        quic::{
            configure_client::configure_client, configure_server::configure_server,
            quiche_server_loop::server_loop,
        },
        types::block_meta::SlotMeta,
    };

    use super::client_loop;

    #[test]
    fn test_send_and_recieve_of_large_account_with_client_loop() {
        tracing_subscriber::fmt::init();
        // Setup the event loop.
        let socket_addr = SocketAddr::from_str("0.0.0.0:10900").unwrap();

        let port = 10900;
        let maximum_concurrent_streams = 100;

        let message_1 = ChannelMessage::Slot(
            3,
            2,
            solana_sdk::commitment_config::CommitmentLevel::Confirmed,
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
            let config = configure_server(maximum_concurrent_streams, 20_000_000, 1).unwrap();
            if let Err(e) = server_loop(
                config,
                socket_addr,
                rx_sent_queue,
                CompressionType::Lz4Fast(8),
                true,
                maximum_concurrent_streams,
            ) {
                println!("Server loop closed by error : {e}");
            }
        });

        // client loop
        let server_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let (client_sx_queue, rx_sent_queue) = mio_channel::channel();
        let (sx_recv_queue, client_rx_queue) = mpsc::channel();

        let _client_loop_jh = std::thread::spawn(move || {
            let client_config =
                configure_client(maximum_concurrent_streams, 20_000_000, 1).unwrap();
            let socket_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
            let is_connected = Arc::new(AtomicBool::new(false));
            if let Err(e) = client_loop(
                client_config,
                socket_addr,
                server_addr,
                rx_sent_queue,
                sx_recv_queue,
                is_connected,
                maximum_concurrent_streams,
            ) {
                println!("client stopped with error {e}");
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
                commitment_level: solana_sdk::commitment_config::CommitmentLevel::Confirmed
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

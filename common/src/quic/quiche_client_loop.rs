use std::net::SocketAddr;

use crate::{
    message::Message,
    quic::{
        configure_server::MAX_DATAGRAM_SIZE, quiche_reciever::recv_message,
        quiche_sender::send_message,
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

    while let Err(e) = socket.send_to(&out[..write], send_info.to) {
        bail!("send() failed: {:?}", e);
    }

    let mut stream_send_id = 0;
    let mut buf = [0; 65535];
    let mut out = [0; MAX_DATAGRAM_SIZE];

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

        let network_updates = events.iter().any(|x| x.token().0 == 0);
        let channel_updates = events.iter().any(|x| x.token().0 == 1);

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
            for stream in conn.readable() {
                let message = recv_message(&mut conn, stream);
                match message {
                    Ok(message) => {
                        message_recv_queue.send(message)?;
                    }
                    Err(e) => {
                        log::error!("Error recieving message : {e}")
                    }
                }
            }
        }
        // chanel updates
        if channel_updates {
            // channel events
            let message_to_send = message_send_queue.try_recv()?;
            stream_send_id += 1;
            if let Err(e) = send_message(&mut conn, stream_send_id, &message_to_send) {
                log::error!("Error sending message on stream : {}", e);
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
        sync::mpsc,
    };

    use itertools::Itertools;
    use quiche::ConnectionId;
    use ring::rand::SystemRandom;
    use std::net::UdpSocket;

    use crate::{
        message::Message,
        quic::{
            configure_client::configure_client,
            configure_server::{configure_server, MAX_DATAGRAM_SIZE},
            quiche_reciever::recv_message,
            quiche_sender::send_message,
            quiche_utils::{mint_token, validate_token},
        },
        types::{account::Account, block_meta::SlotMeta},
    };

    use super::client_loop;

    #[test]
    fn test_send_and_recieve_of_large_account_with_client_loop() {
        let mut config = configure_server(1, 100000, 1).unwrap();

        // Setup the event loop.
        let socket_addr = SocketAddr::from_str("0.0.0.0:0").unwrap();
        let socket = UdpSocket::bind(socket_addr).unwrap();

        let port = socket.local_addr().unwrap().port();
        let local_addr = socket.local_addr().unwrap();

        let account = Account::get_account_for_test(123456, 10_000_000);
        let message_1 = Message::SlotMsg(SlotMeta {
            slot: 1,
            parent: 0,
            commitment_level: solana_sdk::commitment_config::CommitmentLevel::Confirmed,
        });
        let message_2 = Message::AccountMsg(account);
        let message_3 = Message::SlotMsg(SlotMeta {
            slot: 4,
            parent: 3,
            commitment_level: solana_sdk::commitment_config::CommitmentLevel::Processed,
        });

        let jh = {
            let message_1 = message_1.clone();
            let message_2 = message_2.clone();
            let message_3 = message_3.clone();
            let server_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
            let (sx_sent_queue, rx_sent_queue) = mio_channel::channel();
            let (sx_recv_queue, rx_recv_queue) = mpsc::channel();
            std::thread::spawn(move || {
                let jh = std::thread::spawn(move || {
                    let client_config = configure_client(1, 12_000_000, 10).unwrap();
                    let socket_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
                    if let Err(e) = client_loop(
                        client_config,
                        socket_addr,
                        server_addr,
                        rx_sent_queue,
                        sx_recv_queue,
                    ) {
                        println!("client stopped with error {e}");
                    }
                });
                sx_sent_queue.send(message_1).unwrap();
                let rx_message = rx_recv_queue.recv().unwrap();
                assert_eq!(rx_message, message_2);
                println!("verified second message");
                sx_sent_queue.send(message_3).unwrap();
                let rx_message = rx_recv_queue.recv().unwrap();
                assert_eq!(rx_message, message_2);
                println!("verified fourth message");
                jh.join().unwrap();
            })
        };

        loop {
            let mut buf = [0; 65535];
            let mut out = [0; MAX_DATAGRAM_SIZE];

            let (len, from) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(e) => {
                    panic!("recv() failed: {:?}", e);
                }
            };
            println!("recieved first packet");

            log::debug!("got {} bytes", len);

            let pkt_buf = &mut buf[..len];

            // Parse the QUIC packet's header.
            let hdr = match quiche::Header::from_slice(pkt_buf, quiche::MAX_CONN_ID_LEN) {
                Ok(header) => header,

                Err(e) => {
                    panic!("Parsing packet header failed: {:?}", e);
                }
            };
            let rng = SystemRandom::new();
            let conn_id_seed = ring::hmac::Key::generate(ring::hmac::HMAC_SHA256, &rng).unwrap();
            let conn_id = ring::hmac::sign(&conn_id_seed, &hdr.dcid);
            let conn_id = &conn_id.as_ref()[..quiche::MAX_CONN_ID_LEN];
            let conn_id: ConnectionId<'static> = conn_id.to_vec().into();

            if hdr.ty != quiche::Type::Initial {
                panic!("Packet is not Initial");
            }

            if !quiche::version_is_supported(hdr.version) {
                log::warn!("Doing version negotiation");

                let len = quiche::negotiate_version(&hdr.scid, &hdr.dcid, &mut out).unwrap();

                let out = &out[..len];

                if let Err(e) = socket.send_to(out, from) {
                    panic!("send() failed: {:?}", e);
                }
            }

            let mut scid = [0; quiche::MAX_CONN_ID_LEN];
            scid.copy_from_slice(&conn_id);

            let scid = quiche::ConnectionId::from_ref(&scid);

            // Token is always present in Initial packets.
            let token = hdr.token.as_ref().unwrap();

            println!("token: {}", token.iter().map(|x| x.to_string()).join(", "));

            // Do stateless retry if the client didn't send a token.
            if token.is_empty() {
                log::warn!("Doing stateless retry");

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
                    panic!("send() failed: {:?}", e);
                } else {
                    continue;
                }
            }
            let odcid = validate_token(&from, token);
            // The token was not valid, meaning the retry failed, so
            // drop the packet.
            if odcid.is_none() {
                panic!("Invalid address validation token");
            }

            if scid.len() != hdr.dcid.len() {
                panic!("Invalid destination connection ID");
            }

            // Reuse the source connection ID we sent in the Retry packet,
            // instead of changing it again.
            let scid = hdr.dcid.clone();

            log::debug!("New connection: dcid={:?} scid={:?}", hdr.dcid, scid);

            let mut conn =
                quiche::accept(&scid, odcid.as_ref(), local_addr, from, &mut config).unwrap();

            let r_m = recv_message(&mut conn, 1).unwrap();
            assert_eq!(r_m, message_1);
            println!("verified first message");

            send_message(&mut conn, 1, &message_2).unwrap();

            let r_m = recv_message(&mut conn, 2).unwrap();
            assert_eq!(r_m, message_3);
            println!("verified third message");

            send_message(&mut conn, 2, &message_2).unwrap();
            conn.close(true, 0, b"not required").unwrap();
            jh.join().unwrap();
            break;
        }
    }
}

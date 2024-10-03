use quic_geyser_common::stream_manager::StreamSender;
use ring::rand::SecureRandom;
use std::collections::BTreeMap;

pub fn validate_token<'a>(
    src: &std::net::SocketAddr,
    token: &'a [u8],
) -> Option<quiche::ConnectionId<'a>> {
    if token.len() < 6 {
        return None;
    }

    if &token[..6] != b"quiche" {
        return None;
    }

    let token = &token[6..];

    let addr = match src.ip() {
        std::net::IpAddr::V4(a) => a.octets().to_vec(),
        std::net::IpAddr::V6(a) => a.octets().to_vec(),
    };

    if token.len() < addr.len() || &token[..addr.len()] != addr.as_slice() {
        return None;
    }

    Some(quiche::ConnectionId::from_ref(&token[addr.len()..]))
}

pub fn mint_token(hdr: &quiche::Header, src: &std::net::SocketAddr) -> Vec<u8> {
    let mut token = Vec::new();

    token.extend_from_slice(b"quiche");

    let addr = match src.ip() {
        std::net::IpAddr::V4(a) => a.octets().to_vec(),
        std::net::IpAddr::V6(a) => a.octets().to_vec(),
    };

    token.extend_from_slice(&addr);
    token.extend_from_slice(&hdr.dcid);

    token
}

pub fn is_bidi(stream_id: u64) -> bool {
    (stream_id & 0x2) == 0
}

pub fn get_next_bidi(mut current_stream_id: u64, max_number_of_streams: u64) -> u64 {
    loop {
        for stream_id in current_stream_id + 1.. {
            if stream_id >= max_number_of_streams {
                break;
            }
            if is_bidi(stream_id) {
                return stream_id;
            }
        }
        current_stream_id = 0;
    }
}

pub fn is_unidi(stream_id: u64, is_server: bool) -> bool {
    (stream_id & 0x1) == (is_server as u64)
}

pub fn get_next_unidi(
    mut current_stream_id: u64,
    is_server: bool,
    max_number_of_streams: u64,
) -> u64 {
    loop {
        for stream_id in current_stream_id + 1.. {
            if stream_id >= max_number_of_streams {
                break;
            }

            if is_unidi(stream_id, is_server) && !is_bidi(stream_id) {
                return stream_id;
            }
        }
        current_stream_id = 0;
    }
}

pub fn handle_path_events(conn: &mut quiche::Connection) {
    while let Some(qe) = conn.path_event_next() {
        match qe {
            quiche::PathEvent::New(local_addr, peer_addr) => {
                log::info!(
                    "{} Seen new path ({}, {})",
                    conn.trace_id(),
                    local_addr,
                    peer_addr
                );

                // Directly probe the new path.
                conn.probe_path(local_addr, peer_addr)
                    .expect("cannot probe");
            }

            quiche::PathEvent::Validated(local_addr, peer_addr) => {
                log::info!(
                    "{} Path ({}, {}) is now validated",
                    conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }

            quiche::PathEvent::FailedValidation(local_addr, peer_addr) => {
                log::info!(
                    "{} Path ({}, {}) failed validation",
                    conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }

            quiche::PathEvent::Closed(local_addr, peer_addr) => {
                log::info!(
                    "{} Path ({}, {}) is now closed and unusable",
                    conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }

            quiche::PathEvent::ReusedSourceConnectionId(cid_seq, old, new) => {
                log::info!(
                    "{} Peer reused cid seq {} (initially {:?}) on {:?}",
                    conn.trace_id(),
                    cid_seq,
                    old,
                    new
                );
            }

            quiche::PathEvent::PeerMigrated(local_addr, peer_addr) => {
                log::info!(
                    "{} Connection migrated to ({}, {})",
                    conn.trace_id(),
                    local_addr,
                    peer_addr
                );
            }
        }
    }
}

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

// 16 MB per buffer
pub const BUFFER_LEN: usize = 24 * 1024 * 1024;
pub type StreamSenderWithDefaultCapacity = StreamSender<BUFFER_LEN>;
pub type StreamSenderMap = BTreeMap<u64, Box<StreamSenderWithDefaultCapacity>>;

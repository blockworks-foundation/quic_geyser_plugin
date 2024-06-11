use std::collections::HashMap;

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

pub struct PartialResponse {
    pub binary: Vec<u8>,
    pub written: usize,
}

pub type PartialResponses = HashMap<u64, PartialResponse>;

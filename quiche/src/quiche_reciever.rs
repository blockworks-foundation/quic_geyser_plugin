use std::collections::HashMap;

use anyhow::bail;

use quic_geyser_common::{defaults::MAX_DATAGRAM_SIZE, message::Message};

pub fn convert_binary_to_message(bytes: Vec<u8>) -> anyhow::Result<Message> {
    Ok(bincode::deserialize::<Message>(&bytes)?)
}

pub type ReadStreams = HashMap<u64, Vec<u8>>;

pub fn recv_message(
    connection: &mut quiche::Connection,
    read_streams: &mut ReadStreams,
    stream_id: u64,
) -> anyhow::Result<Option<Message>> {
    let mut total_buf = match read_streams.remove(&stream_id) {
        Some(buf) => buf,
        None => vec![],
    };
    loop {
        let mut buf = [0; MAX_DATAGRAM_SIZE];
        match connection.stream_recv(stream_id, &mut buf) {
            Ok((read, fin)) => {
                log::debug!("read {} on stream {}", read, stream_id);
                total_buf.extend_from_slice(&buf[..read]);
                if fin {
                    log::debug!("fin stream : {}", stream_id);
                    match bincode::deserialize::<Message>(&total_buf) {
                        Ok(message) => {
                            log::debug!("error : {message:?}");
                            return Ok(Some(message));
                        }
                        Err(e) => {
                            log::error!("error deserializing");
                            bail!("Error deserializing stream {stream_id} error: {e:?}");
                        }
                    }
                }
            }
            Err(e) => {
                match &e {
                    quiche::Error::Done => {
                        // will be tried again later
                        log::debug!("stream saved : {}. len: {}", stream_id, total_buf.len());
                        read_streams.insert(stream_id, total_buf);
                        return Ok(None);
                    }
                    _ => {
                        bail!("read error on stream : {}, error: {}", stream_id, e);
                    }
                }
            }
        }
    }
}

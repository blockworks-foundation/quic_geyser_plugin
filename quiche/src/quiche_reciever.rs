use anyhow::bail;
use quic_geyser_common::{defaults::MAX_DATAGRAM_SIZE, message::Message};
use std::collections::HashMap;

pub type ReadStreams = HashMap<u64, Vec<u8>>;

pub fn recv_message(
    connection: &mut quiche::Connection,
    read_streams: &mut ReadStreams,
    stream_id: u64,
) -> anyhow::Result<Option<Vec<Message>>> {
    let mut buf = [0; MAX_DATAGRAM_SIZE];
    if let Some(total_buf) = read_streams.get_mut(&stream_id) {
        loop {
            match connection.stream_recv(stream_id, &mut buf) {
                Ok((read, _)) => {
                    log::trace!("read {} on stream {}", read, stream_id);
                    total_buf.extend_from_slice(&buf[..read]);
                }
                Err(e) => match &e {
                    quiche::Error::Done => {
                        let mut messages = vec![];
                        if let Some((message, size)) = Message::from_binary_stream(&total_buf) {
                            total_buf.drain(..size);
                            messages.push(message);
                        }
                        return Ok(if messages.is_empty() {
                            None
                        } else {
                            Some(messages)
                        });
                    }
                    _ => {
                        bail!("read error on stream : {}, error: {}", stream_id, e);
                    }
                },
            }
        }
    } else {
        let mut total_buf = vec![];
        total_buf.reserve(1350);
        loop {
            match connection.stream_recv(stream_id, &mut buf) {
                Ok((read, _)) => {
                    log::trace!("read {} on stream {}", read, stream_id);
                    total_buf.extend_from_slice(&buf[..read]);
                }
                Err(e) => match &e {
                    quiche::Error::Done => {
                        let mut messages = vec![];
                        if let Some((message, size)) = Message::from_binary_stream(&total_buf) {
                            total_buf.drain(..size);
                            messages.push(message);
                        }
                        read_streams.insert(stream_id, total_buf);
                        return Ok(if messages.is_empty() {
                            None
                        } else {
                            Some(messages)
                        });
                    }
                    _ => {
                        bail!("read error on stream : {}, error: {}", stream_id, e);
                    }
                },
            }
        }
    }
}

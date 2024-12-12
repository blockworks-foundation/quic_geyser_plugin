use anyhow::bail;
use quic_geyser_common::{
    defaults::MAX_DATAGRAM_SIZE, message::Message, stream_manager::StreamBuffer,
};
use std::collections::BTreeMap;

const BUFFER_SIZE: usize = 32 * 1024 * 1024;
pub type ReadStreams = BTreeMap<u64, StreamBuffer<BUFFER_SIZE>>;

pub fn recv_message(
    connection: &mut quiche::Connection,
    read_streams: &mut ReadStreams,
    stream_id: u64,
) -> anyhow::Result<Option<Vec<Vec<u8>>>> {
    let mut buf = [0; MAX_DATAGRAM_SIZE];
    if let Some(total_buf) = read_streams.get_mut(&stream_id) {
        loop {
            match connection.stream_recv(stream_id, &mut buf) {
                Ok((read, _)) => {
                    log::trace!("read {} on stream {}", read, stream_id);
                    total_buf.append_bytes(&buf[..read]);
                }
                Err(e) => match &e {
                    quiche::Error::Done => {
                        let mut messages = vec![];

                        while let Some((message, size)) =
                            Message::from_binary_stream_binary(total_buf.as_slices().0)
                        {
                            total_buf.consume(size);
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
        let mut total_buf = StreamBuffer::<BUFFER_SIZE>::new();
        loop {
            match connection.stream_recv(stream_id, &mut buf) {
                Ok((read, _)) => {
                    log::trace!("read {} on stream {}", read, stream_id);
                    total_buf.append_bytes(&buf[..read]);
                }
                Err(e) => match &e {
                    quiche::Error::Done => {
                        let mut messages = vec![];
                        while let Some((message, size)) =
                            Message::from_binary_stream_binary(total_buf.as_slices().0)
                        {
                            total_buf.consume(size);
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

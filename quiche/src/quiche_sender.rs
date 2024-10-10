use crate::quiche_utils::StreamBufferMap;
use quic_geyser_common::stream_manager::StreamBuffer;
use quiche::Connection;

// return if connection has finished writing
pub fn send_message<const BUFFER_LEN: usize>(
    connection: &mut Connection,
    stream_sender_map: &mut StreamBufferMap<BUFFER_LEN>,
    stream_id: u64,
    mut message: Vec<u8>,
) -> std::result::Result<(), quiche::Error> {
    if let Some(stream_sender) = stream_sender_map.get_mut(&stream_id) {
        if stream_sender.is_empty() {
            let written = match connection.stream_send(stream_id, &message, false) {
                Ok(v) => v,
                Err(quiche::Error::Done) => 0,
                Err(e) => {
                    return Err(e);
                }
            };

            log::debug!("dispatched {} on stream id : {}", written, stream_id);
            if written < message.len() {
                log::debug!("appending bytes : {}", message.len() - written);
                message.drain(..written);
                if !stream_sender.append_bytes(&message) {
                    return Err(quiche::Error::BufferTooShort);
                }
            }
        } else if !stream_sender.append_bytes(&message) {
            return Err(quiche::Error::BufferTooShort);
        }
    } else {
        let written = match connection.stream_send(stream_id, &message, false) {
            Ok(v) => v,
            Err(quiche::Error::Done) => 0,
            Err(e) => {
                return Err(e);
            }
        };
        log::debug!(
            "dispatched {} and created new buffer on stream id : {}",
            written,
            stream_id
        );
        message.drain(..written);
        let mut new_stream_sender = StreamBuffer::<BUFFER_LEN>::new();
        if !new_stream_sender.append_bytes(&message) {
            return Err(quiche::Error::BufferTooShort);
        }
        stream_sender_map.insert(stream_id, new_stream_sender);
    }
    Ok(())
}

/// Handles newly writable streams.
pub fn handle_writable<const BUFFER_LEN: usize>(
    conn: &mut quiche::Connection,
    stream_sender_map: &mut StreamBufferMap<BUFFER_LEN>,
    stream_id: u64,
) -> std::result::Result<(), quiche::Error> {
    if let Some(stream_sender) = stream_sender_map.get_mut(&stream_id) {
        let (s1, _s2) = stream_sender.as_slices();
        if !s1.is_empty() {
            match conn.stream_send(stream_id, s1, false) {
                Ok(written) => {
                    if written > 0 {
                        stream_sender.consume(written);
                    }
                }
                Err(quiche::Error::Done) => {
                    //  above
                    return Err(quiche::Error::Done);
                }
                Err(e) => {
                    log::error!(
                        "{} stream id :{stream_id} send failed {e:?}",
                        conn.trace_id()
                    );
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

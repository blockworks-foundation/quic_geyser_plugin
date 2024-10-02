use std::sync::Arc;

use crate::quiche_utils::StreamSenderMap;
use quic_geyser_common::message::Message;
use quiche::Connection;

pub fn convert_to_binary(message: &Message) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(&message)?)
}

// return if connection has finished writing
pub fn send_message(
    connection: &mut Connection,
    stream_sender_map: &mut StreamSenderMap,
    stream_id: u64,
    message: Arc<Vec<u8>>,
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

            log::trace!("dispatched {} on stream id : {}", written, stream_id);
            assert!(stream_sender.append_bytes(&message[written..]));
        } else {
            stream_sender.append_bytes(&message);
        }
    }
    Ok(())
}

/// Handles newly writable streams.
pub fn handle_writable(
    conn: &mut quiche::Connection,
    stream_sender_map: &mut StreamSenderMap,
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
    } else {
        log::error!("No writable stream : {stream_id:?}");
    }
    Ok(())
}

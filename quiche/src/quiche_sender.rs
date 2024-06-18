use crate::{
    quiche_utils::{PartialResponse, PartialResponses},
    stream_manager::StreamManager,
};
use quic_geyser_common::message::Message;
use quiche::Connection;

pub fn convert_to_binary(message: &Message) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(&message)?)
}

// return if connection has finished writing
pub fn send_message(
    connection: &mut Connection,
    partial_responses: &mut PartialResponses,
    stream_id: u64,
    message: &Vec<u8>,
) -> std::result::Result<(), quiche::Error> {
    let written = match connection.stream_send(stream_id, message, true) {
        Ok(v) => v,
        Err(quiche::Error::Done) => 0,
        Err(e) => {
            return Err(e);
        }
    };
    log::trace!("dispatched {} on stream id : {}", written, stream_id);

    if written < message.len() {
        let response = PartialResponse {
            binary: message[written..].to_vec(),
            written,
        };
        partial_responses.insert(stream_id, response);
    }
    Ok(())
}

/// Handles newly writable streams.
pub fn handle_writable(
    conn: &mut quiche::Connection,
    partial_responses: &mut PartialResponses,
    stream_manager: &mut StreamManager,
    stream_id: u64,
) -> std::result::Result<(), quiche::Error> {
    log::trace!("{} stream {} is writable", conn.trace_id(), stream_id);

    let resp = match partial_responses.get_mut(&stream_id) {
        Some(s) => s,
        None => {
            // stream has finished
            match conn.stream_shutdown(stream_id, quiche::Shutdown::Write, 0) {
                Ok(()) => {
                    stream_manager.reset_stream(stream_id);
                }
                Err(e) => {
                    log::error!("error shutting down the stream : {e:?}")
                }
            }
            return Ok(());
        }
    };
    let body = &resp.binary;

    let written = match conn.stream_send(stream_id, body, true) {
        Ok(v) => v,
        Err(quiche::Error::Done) => {
            // done writing
            return Ok(());
        }
        Err(e) => {
            partial_responses.remove(&stream_id);

            log::error!(
                "{} stream id :{stream_id} send failed {e:?}",
                conn.trace_id()
            );
            return Err(e);
        }
    };

    if written == 0 {
        return Ok(());
    }

    if written == resp.binary.len() {
        partial_responses.remove(&stream_id);
    } else {
        resp.binary = resp.binary[written..].to_vec();
        resp.written += written;
    }
    Ok(())
}

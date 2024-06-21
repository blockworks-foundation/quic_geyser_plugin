use crate::quiche_utils::{PartialResponse, PartialResponses};
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
    message: &[u8],
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
    stream_id: u64,
) -> std::result::Result<(), quiche::Error> {
    let resp = match partial_responses.get_mut(&stream_id) {
        Some(s) => s,
        None => {
            if let Err(e) = conn.stream_shutdown(stream_id, quiche::Shutdown::Write, 0) {
                log::error!("error shutting down stream {stream_id:?}, error :{e}");
            }
            return Ok(());
        }
    };
    let body = &resp.binary;

    let written = match conn.stream_send(stream_id, body, true) {
        Ok(v) => v,
        Err(quiche::Error::Done) => {
            //  above
            return Err(quiche::Error::Done);
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
        // match conn.stream_send(stream_id, b"", true) {
        //     Ok(_) => {}
        //     Err(quiche::Error::Done) => {}
        //     Err(e) => {
        //         log::error!("{} fin stream failed {:?}", conn.trace_id(), e);
        //     }
        // }
    } else {
        resp.binary = resp.binary[written..].to_vec();
        resp.written += written;
    }
    Ok(())
}

use super::quiche_utils::PartialResponses;
use crate::{message::Message, quic::quiche_utils::PartialResponse};
use quiche::Connection;

pub fn convert_to_binary(message: &Message) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(&message)?)
}

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
    } else {
        // match connection.stream_send(stream_id, &[], true) {
        //     Ok(_) => {}
        //     Err(quiche::Error::Done) => {}
        //     Err(e) => {
        //         return Err(e);
        //     }
        // }
    }
    Ok(())
}

/// Handles newly writable streams.
pub fn handle_writable(
    conn: &mut quiche::Connection,
    partial_responses: &mut PartialResponses,
    stream_id: u64,
) {
    log::trace!("{} stream {} is writable", conn.trace_id(), stream_id);

    if !partial_responses.contains_key(&stream_id) {
        return;
    }

    let resp = partial_responses
        .get_mut(&stream_id)
        .expect("should have a stream id");
    let body = &resp.binary;

    let written = match conn.stream_send(stream_id, body, true) {
        Ok(v) => v,
        Err(quiche::Error::Done) => 0,
        Err(e) => {
            partial_responses.remove(&stream_id);

            log::error!("{} stream send failed {:?}", conn.trace_id(), e);
            return;
        }
    };

    if written == 0 {
        return;
    }

    if written == resp.binary.len() {
        log::debug!("fin writing stream : {}", stream_id);
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
}

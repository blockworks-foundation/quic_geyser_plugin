use std::sync::Arc;

use crate::quiche_utils::{PartialResponse, PartialResponses};
use prometheus::{opts, register_int_gauge, IntGauge};
use quic_geyser_common::message::Message;
use quiche::Connection;

lazy_static::lazy_static!(
    static ref NUMBER_OF_PARTIAL_RESPONSES: IntGauge =
       register_int_gauge!(opts!("quic_plugin_nb_streams_open", "Number of streams that are open")).unwrap();
);

pub fn convert_to_binary(message: &Message) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(&message)?)
}

// return if connection has finished writing
pub fn send_message(
    connection: &mut Connection,
    partial_responses: &mut PartialResponses,
    stream_id: u64,
    message: Arc<Vec<u8>>,
) -> std::result::Result<(), quiche::Error> {
    let written = match connection.stream_send(stream_id, &message, true) {
        Ok(v) => v,
        Err(quiche::Error::Done) => 0,
        Err(e) => {
            return Err(e);
        }
    };

    log::trace!("dispatched {} on stream id : {}", written, stream_id);

    if written < message.len() {
        let response = PartialResponse { message, written };
        NUMBER_OF_PARTIAL_RESPONSES.inc();
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

    let written = match conn.stream_send(stream_id, &resp.message[resp.written..], true) {
        Ok(v) => v,
        Err(quiche::Error::Done) => {
            //  above
            return Err(quiche::Error::Done);
        }
        Err(e) => {
            NUMBER_OF_PARTIAL_RESPONSES.dec();
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

    if resp.written + written == resp.message.len() {
        NUMBER_OF_PARTIAL_RESPONSES.dec();
        partial_responses.remove(&stream_id);
    } else {
        resp.written += written;
    }
    Ok(())
}

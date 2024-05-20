use itertools::Itertools;
use quiche::Connection;

use crate::message::Message;

use super::configure_server::MAX_DATAGRAM_SIZE;

pub fn convert_to_binary(message: &Message) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(&message)?)
}

pub fn send_message(
    connection: &mut Connection,
    stream_id: u64,
    message: &Message,
) -> anyhow::Result<()> {
    let binary = convert_to_binary(message)?;
    let chunks = binary.chunks(MAX_DATAGRAM_SIZE).collect_vec();
    let nb_chunks = chunks.len();
    for (index, buf) in chunks.iter().enumerate() {
        connection.stream_send(stream_id, buf, index + 1 == nb_chunks)?;
    }
    Ok(())
}

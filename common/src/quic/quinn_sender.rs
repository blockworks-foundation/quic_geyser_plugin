use anyhow::bail;
use quinn::RecvStream;

use crate::message::Message;

pub fn convert_binary_to_message(bytes: Vec<u8>) -> anyhow::Result<Message> {
    Ok(bincode::deserialize::<Message>(&bytes)?)
}

pub async fn recv_message(mut recv_stream: RecvStream) -> anyhow::Result<Message> {
    let chunk = recv_stream.read_chunk(8, true).await?;
    if let Some(chunk) = chunk {
        assert!(chunk.offset == 0);
        let size_bytes = chunk.bytes.to_vec();
        assert!(size_bytes.len() == 8);

        let size_bytes: [u8; 8] = size_bytes.try_into().unwrap();
        let size = u64::from_le_bytes(size_bytes) as usize;
        let mut buffer: Vec<u8> = vec![0; size];
        while let Some(data) = recv_stream.read_chunk(size, false).await? {
            let bytes = data.bytes.to_vec();
            let begin_offset = data.offset as usize;
            let end_offset = data.offset as usize + data.bytes.len();
            buffer[begin_offset..end_offset].copy_from_slice(&bytes);
        }
        convert_binary_to_message(buffer)
    } else {
        bail!("Stream was finished")
    }
}
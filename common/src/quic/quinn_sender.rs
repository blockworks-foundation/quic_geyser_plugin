use quinn::SendStream;

use crate::message::Message;

pub fn convert_to_binary(message: Message) -> anyhow::Result<Vec<u8>> {
    let mut binary = bincode::serialize(&message)?;
    let size = binary.len() as u64;
    // prepend size to the binary object
    let size_bytes = size.to_le_bytes().to_vec();
    binary.splice(0..0, size_bytes);
    Ok(binary)
}

pub async fn send_message(mut send_stream: SendStream, message: Message) -> anyhow::Result<()> {
    let binary = convert_to_binary(message)?;
    send_stream.write_all(&binary).await?;
    send_stream.finish().await?;
    Ok(())
}

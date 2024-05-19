use std::time::Duration;

use anyhow::bail;
use quinn::RecvStream;

use crate::message::Message;

pub fn convert_binary_to_message(bytes: Vec<u8>) -> anyhow::Result<Message> {
    Ok(bincode::deserialize::<Message>(&bytes)?)
}

pub async fn recv_size(
    recv_stream: &mut RecvStream,
    timeout_in_seconds: u64,
) -> anyhow::Result<usize> {
    let mut size_bytes: [u8; 8] = [0; 8];
    let mut remaining: usize = 8;

    while remaining > 0 {
        let chunk = tokio::time::timeout(
            Duration::from_secs(timeout_in_seconds),
            recv_stream.read_chunk(remaining, true),
        )
        .await??;

        match chunk {
            Some(chunk) => {
                let chunk_bytes = chunk.bytes.to_vec();
                let offset = chunk.offset as usize;
                size_bytes[offset..offset + chunk_bytes.len()].copy_from_slice(&chunk_bytes);
                remaining -= chunk_bytes.len();
            }
            None => bail!("Stream was finished with error"),
        }
    }

    Ok(u64::from_le_bytes(size_bytes) as usize)
}

pub async fn recv_message(
    mut recv_stream: RecvStream,
    timeout_in_seconds: u64,
) -> anyhow::Result<Message> {
    let size = recv_size(&mut recv_stream, timeout_in_seconds).await?;
    // 64 MBs accounts maximum
    assert!(size < 64_000_000);

    let mut buffer: Vec<u8> = vec![0; size];

    while let Some(data) =
        tokio::time::timeout(Duration::from_secs(1), recv_stream.read_chunk(size, false)).await??
    {
        let bytes = data.bytes.to_vec();
        let offset = data.offset - 8;
        let begin_offset = offset as usize;
        let end_offset = offset as usize + data.bytes.len();
        buffer[begin_offset..end_offset].copy_from_slice(&bytes);
    }
    convert_binary_to_message(buffer)
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        sync::Arc,
    };

    use crate::{
        message::Message,
        quic::{
            configure_client::configure_client, configure_server::configure_server,
            quinn_reciever::recv_message, quinn_sender::send_message,
        },
        types::account::Account,
    };
    use quinn::{Endpoint, EndpointConfig, TokioRuntime, VarInt};

    #[tokio::test]
    pub async fn test_send_and_recieve_of_small_account() {
        let config = configure_server(1, 100000, 1).unwrap();

        let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
        let port = sock.local_addr().unwrap().port();

        let endpoint = Endpoint::new(
            EndpointConfig::default(),
            Some(config),
            sock,
            Arc::new(TokioRuntime),
        )
        .unwrap();

        let account = Account::get_account_for_test(123456, 2);
        let message = Message::AccountMsg(account);

        let jh = {
            let sent_message = message.clone();

            tokio::spawn(async move {
                let endpoint = configure_client(1).await.unwrap();

                let connecting = endpoint
                    .connect(
                        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
                        "tmp",
                    )
                    .unwrap();
                let connection = connecting.await.unwrap();
                let recv_stream = connection.accept_uni().await.unwrap();
                let recved_message = recv_message(recv_stream, 10).await.unwrap();
                // assert if sent and recieved message match
                assert_eq!(sent_message, recved_message);
            })
        };

        let connecting = endpoint.accept().await.unwrap();
        let connection = connecting.await.unwrap();

        let send_stream = connection.open_uni().await.unwrap();
        send_message(send_stream, &message).await.unwrap();
        jh.await.unwrap();
    }

    #[tokio::test]
    pub async fn test_send_and_recieve_of_large_account() {
        let config = configure_server(1, 100000, 60).unwrap();

        let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
        let port = sock.local_addr().unwrap().port();

        let endpoint = Endpoint::new(
            EndpointConfig::default(),
            Some(config),
            sock,
            Arc::new(TokioRuntime),
        )
        .unwrap();

        let account = Account::get_account_for_test(123456, 100_000_00);
        let message = Message::AccountMsg(account);

        let jh = {
            let sent_message = message.clone();

            tokio::spawn(async move {
                let endpoint = configure_client(0).await.unwrap();

                let connecting = endpoint
                    .connect(
                        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
                        "tmp",
                    )
                    .unwrap();
                let connection = connecting.await.unwrap();
                let send_stream = connection.open_uni().await.unwrap();
                send_message(send_stream, &sent_message).await.unwrap();
            })
        };

        let connecting = endpoint.accept().await.unwrap();
        let connection = connecting.await.unwrap();
        let recv_stream = connection.accept_uni().await.unwrap();

        let recved_message = recv_message(recv_stream, 10).await.unwrap();
        jh.await.unwrap();
        // assert if sent and recieved message match
        assert_eq!(message, recved_message);
        endpoint.close(VarInt::from_u32(0), b"");
    }
}

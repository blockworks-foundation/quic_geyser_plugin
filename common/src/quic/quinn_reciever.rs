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
            let offset = data.offset - 8;
            let begin_offset = offset as usize;
            let end_offset = offset as usize + data.bytes.len();
            buffer[begin_offset..end_offset].copy_from_slice(&bytes);
        }
        convert_binary_to_message(buffer)
    } else {
        bail!("Stream was finished")
    }
}

mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        str::FromStr,
        sync::Arc,
    };

    use quinn::{Endpoint, EndpointConfig, TokioRuntime, VarInt};
    use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Keypair};

    use crate::{
        message::Message,
        quic::{
            configure_client::configure_client, configure_server::configure_server,
            quinn_reciever::recv_message, quinn_sender::send_message,
        },
        types::{account::Account, slot_identifier::SlotIdentifier},
    };

    #[tokio::test]
    pub async fn test_send_and_recieve_of_small_account() {
        let (config, _) = configure_server(
            &Keypair::new(),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            1,
            100000,
            1,
        )
        .unwrap();

        let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
        let port = sock.local_addr().unwrap().port();

        let endpoint = Endpoint::new(
            EndpointConfig::default(),
            Some(config),
            sock,
            Arc::new(TokioRuntime),
        )
        .unwrap();

        let account = Account {
            slot_identifier: SlotIdentifier {
                slot: 12345678,
                blockhash: Hash::new_unique(),
            },
            pubkey: Pubkey::new_unique(),
            data: vec![6; 2],
        };
        let message = Message::AccountMsg(account);

        let jh = {
            let sent_message = message.clone();

            tokio::spawn(async move {
                let endpoint = configure_client(&Keypair::new()).await.unwrap();

                let connecting = endpoint
                    .connect(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port), "tmp")
                    .unwrap();
                let connection = connecting.await.unwrap();
                let send_stream = connection.open_uni().await.unwrap();
                send_message(send_stream, sent_message).await.unwrap();
            })
        };

        let connecting = endpoint.accept().await.unwrap();
        let connection = connecting.await.unwrap();
        let recv_stream = connection.accept_uni().await.unwrap();

        let recved_message = recv_message(recv_stream).await.unwrap();
        jh.await.unwrap();
        // assert if sent and recieved message match
        assert_eq!(message, recved_message);
        endpoint.close(VarInt::from_u32(0), b"");
    }

    #[tokio::test]
    pub async fn test_send_and_recieve_of_large_account() {
        let (config, _) = configure_server(
            &Keypair::new(),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            1,
            100000,
            60,
        )
        .unwrap();

        let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
        let port = sock.local_addr().unwrap().port();

        let endpoint = Endpoint::new(
            EndpointConfig::default(),
            Some(config),
            sock,
            Arc::new(TokioRuntime),
        )
        .unwrap();

        let account = Account {
            slot_identifier: SlotIdentifier {
                slot: 12345678,
                blockhash: Hash::new_unique(),
            },
            pubkey: Pubkey::new_unique(),
            data: vec![6; 100_000_000],
        };
        let message = Message::AccountMsg(account);

        let jh = {
            let sent_message = message.clone();

            tokio::spawn(async move {
                let endpoint = configure_client(&Keypair::new()).await.unwrap();

                let connecting = endpoint
                    .connect(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port), "tmp")
                    .unwrap();
                let connection = connecting.await.unwrap();
                let send_stream = connection.open_uni().await.unwrap();
                send_message(send_stream, sent_message).await.unwrap();
            })
        };

        let connecting = endpoint.accept().await.unwrap();
        let connection = connecting.await.unwrap();
        let recv_stream = connection.accept_uni().await.unwrap();

        let recved_message = recv_message(recv_stream).await.unwrap();
        jh.await.unwrap();
        // assert if sent and recieved message match
        assert_eq!(message, recved_message);
        endpoint.close(VarInt::from_u32(0), b"");
    }

}
use std::io::BufRead;

pub struct StreamBuffer<const BUFFER_LEN: usize> {
    buffer: Box<circular_buffer::CircularBuffer<BUFFER_LEN, u8>>,
}

#[allow(clippy::new_without_default)]
impl<const BUFFER_LEN: usize> StreamBuffer<BUFFER_LEN> {
    pub fn new() -> StreamBuffer<BUFFER_LEN> {
        StreamBuffer {
            buffer: circular_buffer::CircularBuffer::boxed(),
        }
    }

    pub fn append_bytes(&mut self, bytes: &[u8]) -> bool {
        if self.capacity() > bytes.len() {
            self.buffer.extend_from_slice(bytes);
            true
        } else {
            // not enough capacity
            false
        }
    }

    pub fn as_slices(&self) -> (&[u8], &[u8]) {
        self.buffer.as_slices()
    }

    pub fn consume(&mut self, nb_bytes: usize) -> bool {
        let len = self.buffer.len();
        if len < nb_bytes {
            return false;
        }
        self.buffer.consume(nb_bytes);
        true
    }

    pub fn as_buffer(&self) -> Vec<u8> {
        let (slice1, slice2) = self.as_slices();
        [slice1, slice2].concat()
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn capacity(&self) -> usize {
        BUFFER_LEN - self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    const NEAR_FULL: usize = (BUFFER_LEN * 95) / 100;
    pub fn is_near_full(&self) -> bool {
        self.buffer.len() > Self::NEAR_FULL
    }

    const REQUIRED_CAPACITY: usize = (BUFFER_LEN * 75) / 100;
    pub fn has_more_than_required_capacity(&self) -> bool {
        self.capacity() < Self::REQUIRED_CAPACITY
    }
}

#[cfg(test)]
mod tests {
    use circular_buffer::CircularBuffer;
    use itertools::Itertools;
    use rand::{rngs::ThreadRng, Rng};
    use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};

    use crate::{
        message::Message,
        types::{
            account::Account,
            block_meta::{BlockMeta, SlotMeta},
            slot_identifier::SlotIdentifier,
        },
    };

    use super::StreamBuffer;

    #[test]
    pub fn test_drain_on_circular_buffer() {
        let mut buf = CircularBuffer::<6, char>::from_iter("abcdef".chars());
        let drained = buf.drain(..3).collect::<Vec<char>>();
        assert_eq!(drained, vec!['a', 'b', 'c']);
        buf.extend_from_slice(&"ghi".chars().collect_vec());
        let (s1, s2) = buf.as_slices();
        assert_eq!(s1, vec!['d', 'e', 'f', 'g', 'h', 'i']);
        assert!(s2.is_empty());
        let drained = buf.drain(..3).collect::<Vec<char>>();
        assert_eq!(drained, vec!['d', 'e', 'f']);
    }

    pub fn create_random_message(rng: &mut ThreadRng) -> Message {
        let message_type = rng.gen::<u8>() % 3;

        match message_type {
            0 => Message::SlotMsg(SlotMeta {
                slot: rng.gen(),
                parent: rng.gen(),
                commitment_config: CommitmentConfig::processed(),
            }),
            1 => {
                let data_length = rng.gen_range(10..128);
                let data = (0..data_length).map(|_| rng.gen::<u8>()).collect_vec();
                Message::AccountMsg(Account {
                    slot_identifier: SlotIdentifier { slot: rng.gen() },
                    pubkey: Pubkey::new_unique(),
                    owner: Pubkey::new_unique(),
                    lamports: rng.gen(),
                    executable: rng.gen_bool(0.5),
                    rent_epoch: rng.gen(),
                    write_version: rng.gen(),
                    data,
                    compression_type: crate::compression::CompressionType::None,
                    data_length,
                })
            }
            2 => Message::BlockMetaMsg(BlockMeta {
                parent_blockhash: "jfkjahfkajfnaf".to_string(),
                parent_slot: rng.gen(),
                slot: rng.gen(),
                blockhash: "lkjsahkjhakda".to_string(),
                block_height: rng.gen(),
                rewards: vec![],
                entries_count: rng.gen(),
                executed_transaction_count: rng.gen(),
                block_time: rng.gen(),
            }),
            _ => {
                unreachable!()
            }
        }
    }

    #[test]
    pub fn create_and_consume_random_messages() {
        let mut buffer = StreamBuffer::<3000>::new();
        let mut rng = rand::thread_rng();

        let mut messages_appended = vec![];
        let mut messages_consumed = vec![];
        for _ in 0..1_000_000 {
            let do_append = rng.gen_bool(0.6);
            if do_append {
                let message = create_random_message(&mut rng);
                if buffer.append_bytes(&message.to_binary_stream()) {
                    messages_appended.push(message);
                }
            } else {
                let buf = buffer.as_slices();
                if let Some((message, size)) = Message::from_binary_stream(buf.0) {
                    messages_consumed.push(message);
                    buffer.consume(size);
                }
            }
        }

        while let Some((message, size)) = Message::from_binary_stream(buffer.as_slices().0) {
            messages_consumed.push(message);
            buffer.consume(size);
        }

        println!("Added : {} messages", messages_appended.len());
        assert_eq!(messages_appended, messages_consumed);
    }
}

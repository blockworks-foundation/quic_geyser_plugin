use std::collections::VecDeque;

use crate::quiche_utils::get_next_unidi;

pub struct StreamManager {
    stream_ids: VecDeque<u64>,
    last_stream_id: u64,
    is_server: bool,
}

impl StreamManager {
    pub fn new(size: usize, is_server: bool) -> Self {
        let mut stream_ids = VecDeque::new();
        stream_ids.reserve(size);
        let mut current_stream_id = 0;
        (0..size).for_each(|_| {
            current_stream_id = get_next_unidi(current_stream_id, is_server, u64::MAX);
            stream_ids.push_back(current_stream_id);
        });
        Self {
            stream_ids,
            last_stream_id: current_stream_id,
            is_server,
        }
    }

    pub fn get(&mut self) -> u64 {
        match self.stream_ids.pop_front() {
            Some(value) => value,
            None => {
                self.last_stream_id = get_next_unidi(self.last_stream_id, self.is_server, u64::MAX);
                self.last_stream_id
            }
        }
    }

    pub fn reset_stream(&mut self, stream_id: u64) {
        self.stream_ids.push_back(stream_id);
    }
}

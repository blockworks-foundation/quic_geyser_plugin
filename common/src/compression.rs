use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Lz4Fast(u32),
    Lz4(u32),
}

impl Default for CompressionType {
    fn default() -> Self{
        Self::Lz4Fast(8)
    }
}
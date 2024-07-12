use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SnapshotConfig {
    pub enabled: bool,
    pub load_program_ids_except: Vec<String>,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            load_program_ids_except: vec![
                solana_program::system_program::id().to_string(),
                solana_program::stake::program::id().to_string(),
                solana_program::vote::program::id().to_string(),
            ],
        }
    }
}

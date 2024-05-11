use serde::{Deserialize, Serialize};

use crate::types::account::Account;

#[derive(Serialize, Deserialize, Clone)]
pub enum Message {
    AccountMsg(Account),
}
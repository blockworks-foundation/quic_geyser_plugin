use serde::{Deserialize, Serialize};

use crate::types::account::Account;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Message {
    AccountMsg(Account),
}

use serde::{Deserialize, Serialize};

use crate::{filters::Filter, types::account::Account};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Message {
    AccountMsg(Account),
    Filters(Vec<Filter>), // sent from client to server
}

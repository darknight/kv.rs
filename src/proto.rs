use serde::{Serialize, Deserialize};

///
/// Simple command for interaction between kvs-client & kvs-server
///
#[derive(Debug, Serialize, Deserialize)]
pub enum ReqProto {
    /// `get <KEY>`
    Get(String),
    /// `rm <KEY>`
    Remove(String),
    /// `set <KEY> <VALUE>`
    Set(String, String),
}

///
/// Simple response from kvs-server
///
#[derive(Debug, Serialize, Deserialize)]
pub enum RespProto {
    /// successful response
    OK(Option<String>),
    /// error response
    Error(String)
}

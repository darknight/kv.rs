use serde::{Serialize, Deserialize};

///
/// Simple command for interaction between kvs-client & kvs-server
///
#[derive(Debug, Serialize, Deserialize)]
pub enum Proto {
    /// `get <KEY>`
    Get(String),
    /// `rm <KEY>`
    Remove(String),
    /// `set <KEY> <VALUE>`
    Set(String, String),
}

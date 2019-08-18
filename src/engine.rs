use std::io;
use std::result;
use std::sync::{RwLock, RwLockReadGuard};

///
/// define customized error type
/// refer to this awesome article: https://blog.burntsushi.net/rust-error-handling/
///
#[derive(Debug)]
pub enum KvError {
    /// io error
    IoErr(io::Error),
    /// error from serde_json
    SerdeJsonError(serde_json::Error),
    /// key not found error
    KeyNotFound,
    /// deal with Path error
    DirPathExpected,
    /// path contains unexpected files
    FileMismatchInPath,
    /// log file not found in path
    UnexpectedLogFile,
    /// server side error
    InvalidIpAddr(std::net::AddrParseError),
    /// wrapper of sled engine error
    SledError(sled::Error),
    /// error when acquire RwLock
    LockError
}

impl From<io::Error> for KvError {
    fn from(err: io::Error) -> KvError {
        KvError::IoErr(err)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(err: serde_json::Error) -> KvError {
        KvError::SerdeJsonError(err)
    }
}

impl From<std::net::AddrParseError> for KvError {
    fn from(err: std::net::AddrParseError) -> KvError {
        KvError::InvalidIpAddr(err)
    }
}

impl From<sled::Error> for KvError {
    fn from(err: sled::Error) -> KvError {
        KvError::SledError(err)
    }
}

/// alias
pub type Result<T> = result::Result<T, KvError>;

///
/// defines the storage interface called by KvsServer
///
pub trait KvsEngine: Clone + Send + 'static {

    ///
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    ///
    fn set(&self, key: String, value: String) -> Result<()>;
    ///
    /// Get the string value of a string key. If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    ///
    fn get(&self, key: String) -> Result<Option<String>>;
    ///
    /// Remove a given string key.
    /// Return an error if the key does not exit or value is not read successfully.
    ///
    fn remove(&self, key: String) -> Result<()>;
}

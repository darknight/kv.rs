#![deny(missing_docs)]
//! KvStore library
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::result;
use std::error;
use std::fs;
use std::fs::{File, OpenOptions};

use serde::{Serialize, Deserialize};

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

/// alias
pub type Result<T> = result::Result<T, KvError>;

/// default log path
const LOG_FILE: &'static str = "data.log";

#[derive(Debug, Serialize, Deserialize)]
enum LogEntry {
    Set {
        key: String,
        value: String,
    },
    Remove(String),
}

///
/// core data structure for saving key/value pair
///
pub struct KvStore {
    data: HashMap<String, String>,
    log_file: File,
}

impl Default for KvStore {
    fn default() -> Self {
        KvStore::open(LOG_FILE).expect("Fail to create default KvStore")
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        self.log_file.flush().expect("Fail to drop KvStore before flush data")
    }
}

///
/// implementation of KvStore
///
impl KvStore {

    ///
    /// save key/value pair
    ///
    pub fn set(&mut self, k: String, v: String) -> Result<()> {
        // create log entry, serialize, write to log file
        let entry = LogEntry::Set {
            key: k.clone(),
            value: v.clone(),
        };
        serde_json::to_writer(&self.log_file, &entry)?;
        self.log_file.write("\n".as_bytes())?;
        // set in-memory store
        self.data.insert(k, v);
        Ok(())
    }

    ///
    /// get value by key
    ///
    pub fn get(&self, k: String) -> Result<Option<String>> {
        Ok(self.data.get(&k).map(String::from))
    }

    ///
    /// remove key/value pair from KvStore
    ///
    pub fn remove(&mut self, k: String) -> Result<()> {
        match self.data.remove(&k) {
            None => Err(KvError::KeyNotFound),
            Some(_) => {
                let entry = LogEntry::Remove(k.clone());
                serde_json::to_writer(&self.log_file, &entry)?;
                Ok(())
            }
        }
    }

    ///
    /// return initialized KvStore
    ///
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let p = Self::ensure_path(path.as_ref())?;
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(p)?;
        let mut kv_store = KvStore {
            data: HashMap::new(),
            log_file: file,
        };
        kv_store.load_data()?;
        Ok(kv_store)
    }

    ///
    /// Prepare the file path
    ///
    fn ensure_path(path: &Path) -> Result<PathBuf> {
        if path.is_file() {
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent)?;
                }
            }
            return Ok(path.to_path_buf());
        }
        else {
            if !path.exists() {
                fs::create_dir_all(path)?;
            }
            let new_path = path.join(LOG_FILE);
            return Ok(new_path);
        }
    }

    ///
    /// load a file, replay all the records
    ///
    fn load_data(&mut self) -> Result<()> {
        let mut buf_reader = BufReader::new(&self.log_file);
        for line in buf_reader.lines() {
            let row = line?;
            let entry: LogEntry = serde_json::from_str(row.as_str())?;
            match entry {
                LogEntry::Set {key, value} => self.data.insert(key, value),
                LogEntry::Remove(key) => self.data.remove(&key),
            };
        }
        Ok(())
    }

    ///
    /// print current snapshot of kvstore
    ///
    pub fn dprint(&self) {
        println!("KvStore =>");
        println!("{:?}", self.data);
    }
}

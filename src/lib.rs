#![deny(missing_docs)]
//! KvStore library
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::result;
use std::error;
use std::fs;
use std::fs::{File, OpenOptions};

use serde::{Serialize, Deserialize};
use std::ffi::OsString;
use std::error::Error;

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
    /// server side error
    InvalidIpAddr(std::net::AddrParseError)
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

/// alias
pub type Result<T> = result::Result<T, KvError>;

/// default log file
const LOG_FILE: &'static str = "data.log";
/// max file size (in bytes) before executing compaction or splitting into segments
const MAX_FILE_BYTES: u64 = 1024 * 1024 ;

#[derive(Debug, Serialize, Deserialize)]
enum LogEntry {
    Set {
        key: String,
        value: String,
    },
    Remove(String),
}

///
/// defines the storage interface called by KvsServer
///
pub trait KvsEngine {

    ///
    /// Set the value of a string key to a string.
    /// Return an error if the value is not written successfully.
    ///
    fn set(&mut self, key: String, value: String) -> Result<()>;
    ///
    /// Get the string value of a string key. If the key does not exist, return None.
    /// Return an error if the value is not read successfully.
    ///
    fn get(&mut self, key: String) -> Result<Option<String>>;
    ///
    /// Remove a given string key.
    /// Return an error if the key does not exit or value is not read successfully.
    ///
    fn remove(&mut self, key: String) -> Result<()>;
}

///
/// core data structure for saving key/value pair
///
pub struct KvStore {
    data: HashMap<String, u64>,
    path_buf: PathBuf,
    log_file: File,
    current_offset: u64,
}

impl Default for KvStore {
    fn default() -> Self {
        KvStore::open(".").expect("Fail to create default KvStore")
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
        self.check_and_do_compaction()?;
        self.set_internal(k, v)
    }

    ///
    /// internal set without compaction
    ///
    pub fn set_internal(&mut self, k: String, v: String) -> Result<()> {
        // create log entry, serialize, write to log file
        let entry = LogEntry::Set {
            key: k.clone(),
            value: v.clone(),
        };
        let mut entry_str = serde_json::to_string(&entry)?;
        entry_str.push_str("\n");
        self.log_file.write(entry_str.as_bytes())?;
        // set in-memory offset
        self.data.insert(k, self.current_offset);
        self.current_offset += entry_str.as_bytes().len() as u64;
        Ok(())
    }

    ///
    /// get value by key
    ///
    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        match self.data.get(&k) {
            None => Ok(None),
            Some(&offset) => {
                self.log_file.seek(SeekFrom::Start(offset))?;
                let mut buf_reader = BufReader::new(&self.log_file);
                let mut raw = String::new();
                buf_reader.read_line(&mut raw)?;
                if let LogEntry::Set { key, value} = serde_json::from_str(raw.as_str())? {
                    Ok(Some(value))
                } else {
                    Err(KvError::KeyNotFound)
                }
            }
        }
    }

    ///
    /// remove key/value pair from KvStore
    ///
    pub fn remove(&mut self, k: String) -> Result<()> {
        self.check_and_do_compaction()?;
        self.remove_internal(k)
    }

    ///
    /// internal remove without compaction
    ///
    fn remove_internal(&mut self, k: String) -> Result<()> {
        match self.data.remove(&k) {
            None => Err(KvError::KeyNotFound),
            Some(_) => {
                let entry = LogEntry::Remove(k.clone());
                let mut entry_str = serde_json::to_string(&entry)?;
                entry_str.push_str("\n");
                self.log_file.write(entry_str.as_bytes())?;
                // set in-memory offset
                self.current_offset += entry_str.as_bytes().len() as u64;
                Ok(())
            }
        }
    }

    ///
    /// return initialized KvStore
    ///
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let p = Self::ensure_path(path.as_ref(), LOG_FILE)?;
        Self::open_internal(p)
    }

    ///
    /// pass in valid file path
    ///
    fn open_internal(file_path: PathBuf) -> Result<Self> {
        let file = Self::open_file(&file_path)?;
        let mut kv_store = KvStore {
            data: HashMap::new(),
            path_buf: file_path,
            log_file: file,
            current_offset: 0u64,
        };
        kv_store.load_data()?;
        Ok(kv_store)
    }

    ///
    /// Prepare the file path
    ///
    fn ensure_path(path: &Path, file_name: &str) -> Result<PathBuf> {
        if path.exists() && path.is_file() {
            return Err(KvError::DirPathExpected);
        }
        fs::create_dir_all(path)?;
        let file_path = path.join(file_name);
        Ok(file_path)
    }

    ///
    /// Open file with Read + Append + Create
    ///
    fn open_file<P: AsRef<Path>>(path: P) -> io::Result<File> { // FIXME: convert to local Result
        OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path.as_ref())
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
                LogEntry::Set {key, value} =>
                    self.data.insert(key, self.current_offset),
                LogEntry::Remove(key) =>
                    self.data.remove(&key),
            };
            self.current_offset += row.as_bytes().len() as u64 + 1; // 1 for newline
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

    ///
    /// check file size and do compaction if file is too large
    /// the strategy currently is blocking set & remove until compaction is completed
    /// action:
    /// 1. create temp KvStore with opening TEMP_LOG_FILE
    /// 2. dump data in current KvStore to temp KvStore
    /// 3. drop temp KvStore
    /// 4. overwrite original file with temp file by renaming
    /// 5. create new file handle for the new file, assign it to current KvStore
    /// 6. return
    ///
    /// TODO: handle stale temp file if compaction fails in the middle
    ///
    fn check_and_do_compaction(&mut self) -> Result<()> {
        let metadata = self.log_file.metadata()?;
        if metadata.len() < MAX_FILE_BYTES {
            return Ok(());
        }
        // path_buf is guaranteed as a file
        let file_name = self.path_buf.file_name().unwrap();
        let mut tmp_file_name = OsString::new();
        tmp_file_name.push(file_name);
        tmp_file_name.push(".tmp");
        let tmp_file_path = self.path_buf.clone().parent().unwrap().join(tmp_file_name);

        let mut temp_kv_store = KvStore::open_internal(tmp_file_path.clone())?;
        let keys: Vec<String> = self.data.keys().map(|k| k.to_string()).collect();
        for key in keys {
            let value_opt = self.get(key.to_string())?;
            let value = value_opt
                .expect(&format!("Key {:?} not found when doing compaction", key));
            temp_kv_store.set_internal(key.to_string(), value)?;
        }
        drop(temp_kv_store);
        fs::rename(tmp_file_path.as_path(), self.path_buf.as_path())?;
        self.log_file = Self::open_file(self.path_buf.as_path())?;
        Ok(())
    }
}

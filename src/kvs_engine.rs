use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::ffi::OsString;
use std::error::Error;

use serde::{Serialize, Deserialize};

use super::engine::{Result, KvsEngine, KvError};
use std::sync::{Arc, RwLock};

/// default log file
const DEFAULT_PATH: &'static str = "./database";
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
/// wrap Store with Arc & RwLock to make it share on multiple thread
/// but with mutation support
///
#[derive(Clone)]
pub struct KvStore {
    store: Arc<RwLock<Store>>,
}

impl Default for KvStore {
    fn default() -> Self {
        KvStore::open(DEFAULT_PATH).expect("Fail to create default KvStore")
    }
}

///
/// implement KvStore
///
impl KvStore {

    ///
    /// return initialized KvStore
    ///
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let store = Store::open(path)?;
        Ok(KvStore {
            store: Arc::new(RwLock::new(store))
        })
    }

}

///
/// core data structure for saving key/value pair
///
pub struct Store {
    data: HashMap<String, u64>,
    path_buf: PathBuf,
    log_file: File,
    current_offset: u64,
}

impl Drop for Store {
    fn drop(&mut self) {
        self.log_file.flush().expect("Fail to drop KvStore before flush data")
    }
}

///
/// implementation of KvStore
///
impl Store {

    ///
    /// internal get
    ///
    fn get_internal(&mut self, k: String) -> Result<Option<String>> {
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
    /// internal set without compaction
    ///
    fn set_internal(&mut self, k: String, v: String) -> Result<()> {
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
    /// return initialized Store
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
        let mut kv_store = Store {
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
    /// In order not to mess up with other engine dir
    /// Path must meet
    /// 1. not exist
    /// 2. exist but not a file and
    ///   a. must be empty
    ///   b. if non-empty, must ONLY contain `LOG_FILE`
    ///   c. return Err for another case
    ///
    fn ensure_path(path: &Path, file_name: &str) -> Result<PathBuf> {
        if path.exists() {
            if path.is_file() {
                return Err(KvError::DirPathExpected);
            }

            let dir_entry: Vec<fs::DirEntry> = fs::read_dir(path)?
                .map(|dir| dir.expect("map DirEntry error"))
                .collect();
            if dir_entry.len() > 1 {
                return Err(KvError::FileMismatchInPath);
            }
            if dir_entry.len() == 1 &&
                &dir_entry[0].file_name().to_str().unwrap_or("") != &LOG_FILE {
                return Err(KvError::UnexpectedLogFile);
            }
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
    /// FIXME: compaction is not working well for benchmark tests
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

        let mut temp_kv_store = Self::open_internal(tmp_file_path.clone())?;
        let keys: Vec<String> = self.data.keys().map(|k| k.to_string()).collect();
        for key in keys {
            let value_opt = self.get_internal(key.to_string())?;
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

impl KvsEngine for KvStore {

    ///
    /// save key/value pair
    ///
    fn set(&self, k: String, v: String) -> Result<()> {
//        self.check_and_do_compaction()?;
        match self.store.write() {
            Ok(mut guard) => {
                guard.set_internal(k, v)
            },
            // TODO: propagate PoisonError
            Err(_) => {
                Err(KvError::LockError)
            }
        }
    }

    ///
    /// get value by key
    ///
    fn get(&self, k: String) -> Result<Option<String>> {
        // TODO: change to `read`, blocked by `seek` internally
        match self.store.write() {
            Ok(mut guard) => {
                guard.get_internal(k)
            },
            // TODO: propagate PoisonError
            Err(_) => {
                Err(KvError::LockError)
            }
        }
    }

    ///
    /// remove key/value pair from KvStore
    ///
    fn remove(&self, k: String) -> Result<()> {
//        self.check_and_do_compaction()?;
        match self.store.write() {
            Ok(mut guard) => {
                guard.remove_internal(k)
            },
            // TODO: propagate PoisonError
            Err(_) => {
                Err(KvError::LockError)
            }
        }
    }

}

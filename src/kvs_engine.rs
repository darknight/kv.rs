use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::ffi::OsString;
use std::error::Error;
use std::thread;

use serde::{Serialize, Deserialize};

use super::engine::{Result, KvsEngine, KvError};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// default log file
const DEFAULT_PATH: &'static str = "./database";
const LOG_FILE: &'static str = "data.log";
/// max file size (in bytes) before executing compaction or splitting into segments
const MAX_FILE_BYTES: u64 = 1024 * 1024;
/// schedule interval for compaction
const COMPACTION_INTERVAL: Duration = Duration::from_secs(5);
/// temporary file for compaction
const COMPACTION_LOG_FILE: &'static str = "data.log.tmp";

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
    compact_thread: Arc<JoinHandle<()>>,
    terminate: Arc<AtomicBool>,
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
        let inner_store = Store::open(path, LOG_FILE)?;
        let store = Arc::new(RwLock::new(inner_store));
        let terminate = Arc::new(AtomicBool::new(false));

        let store_cp = store.clone();
        let terminate_cp = terminate.clone();
        let handle = thread::spawn(move || loop {
            if terminate_cp.load(Ordering::SeqCst) {
                break;
            }
            thread::sleep(COMPACTION_INTERVAL);
            let res = check_and_do_compaction(store_cp.clone());
        });
        Ok(KvStore {
            store,
            compact_thread: Arc::new(handle),
            terminate,
        })
    }

}

///
/// core data structure for saving key/value pair
///
pub struct Store {
    data: HashMap<String, u64>,
    dir_path: PathBuf,
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
    pub fn open<P: AsRef<Path>>(dir: P, file_name: &str) -> Result<Self> {
        let file_path = Self::ensure_path(dir.as_ref(), file_name)?;
        Self::open_internal(dir, file_path)
    }

    ///
    /// pass in valid file path
    ///
    fn open_internal<P: AsRef<Path>>(dir: P, file_path: PathBuf) -> Result<Self> {
        let file = Self::open_file(&file_path)?;
        let mut kv_store = Store {
            data: HashMap::new(),
            dir_path: PathBuf::from(dir.as_ref()),
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

}

///
/// check file size and do compaction if file is too large in a separate thread
/// action:
/// - create temp KvStore with opening COMPACTION_LOG_FILE
/// - dump data in current KvStore to temp KvStore
/// - overwrite original file with temp file by renaming
/// - create new file handle for the new file, assign it to current KvStore
/// - drop temp KvStore
/// - return
///
/// TODO: handle stale temp file if compaction fails in the middle
///
fn check_and_do_compaction(store: Arc<RwLock<Store>>) -> Result<()> {
    match store.write() {
        Ok(mut guard) => {
            let metadata = guard.log_file.metadata()?;
            if metadata.len() < MAX_FILE_BYTES {
                return Ok(());
            }

            let mut tmp_store = Store::open(
                &guard.dir_path,
                COMPACTION_LOG_FILE
            )?;

            let keys: Vec<String> = guard.data.keys().map(|k| k.to_string()).collect();
            for key in keys {
                let value_opt = guard.get_internal(key.to_string())?;
                // FIXME: `expect` will compromise current thread
                let value = value_opt
                    .expect(&format!("Key {:?} not found when doing compaction", key));
                tmp_store.set_internal(key, value)?;
            }

            fs::rename(tmp_store.dir_path.join(COMPACTION_LOG_FILE),
                       guard.dir_path.join(LOG_FILE))?;
            guard.log_file = tmp_store.log_file.try_clone()?;

            drop(tmp_store);
            Ok(())
        },
        Err(_) => {
            Err(KvError::LockError)
        }
    }
}

// TODO: implement lock-free read
impl KvsEngine for KvStore {

    ///
    /// save key/value pair
    ///
    fn set(&self, k: String, v: String) -> Result<()> {
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

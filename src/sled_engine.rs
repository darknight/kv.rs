use std::path::{Path, PathBuf};
use std::fs;

use sled::{Db, IVec};

use super::engine::{Result, KvsEngine, KvError};

/// default log file
const DEFAULT_PATH: &'static str = "./database";

/// Wrapper for sled Db struct
pub struct SledStore {
    db: Db,
}

impl Default for SledStore {
    fn default() -> Self {
        let db = Db::start_default(DEFAULT_PATH).expect("Couldn't create sled Db");
        SledStore { db }
    }
}

impl SledStore {
    ///
    /// return initialized KvStore
    ///
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let p = Self::ensure_path(path.as_ref())?;
        let db = Db::start_default(p)?;
        Ok(SledStore { db })
    }

    ///
    /// Path must meet
    /// 1. not exist
    /// 2. if exist, there must be more than 1 file
    ///
    fn ensure_path(path: &Path) -> Result<PathBuf> {
        if path.exists() {
            if path.is_file() {
                return Err(KvError::DirPathExpected);
            }
            let dir_entry: Vec<fs::DirEntry> = fs::read_dir(path)?
                .map(|dir| dir.expect("map DirEntry error"))
                .collect();
            if dir_entry.len() == 1 {
                return Err(KvError::FileMismatchInPath);
            }
        }
        Ok(path.to_path_buf())
    }
}

impl KvsEngine for SledStore {

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let res = self.db.set(key, IVec::from(value.as_bytes()));
        match res {
            Ok(_) => Ok(()),
            Err(err) => Err(KvError::SledError(err)),
        }
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let res = self.db.get(key);
        match res {
            Ok(None) => Ok(None),
            Ok(Some(ivec)) => Ok(Some(
                String::from_utf8(ivec.to_vec()).expect("value is not utf-8 encoded")
            )),
            Err(err) => Err(KvError::SledError(err)),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        unimplemented!()
    }
}
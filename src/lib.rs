#![deny(missing_docs)]
//! KvStore library
use std::collections::HashMap;

///
/// core data structure for saving key/value pair
///
#[derive(Default)]
pub struct KvStore {
    data: HashMap<String, String>,
}

///
/// implementation of KvStore
///
impl KvStore {
    ///
    /// initialize KvStore
    ///
    /// ```rust
    /// use kvs::KvStore;
    /// let store = KvStore::new();
    /// ```
    ///
    pub fn new() -> KvStore {
        KvStore {
            data: HashMap::new(),
        }
    }

    ///
    /// save key/value pair
    ///
    pub fn set(&mut self, k: String, v: String) {
        self.data.insert(k, v);
    }

    ///
    /// get value by key
    ///
    pub fn get(&self, k: String) -> Option<String> {
        self.data.get(&k).map(String::from)
    }

    ///
    /// remove key/value pair from KvStore
    ///
    pub fn remove(&mut self, k: String) {
        self.data.remove(&k);
    }
}

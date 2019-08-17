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

/// redis proto implementation (in process)
mod resp;
/// simple command, will be replaced in future by resp
pub mod proto;
/// the general engine trait
pub mod engine;
/// sled engine
pub mod sled_engine;
/// kvs engine;
pub mod kvs_engine;
/// thread pool
pub mod thread_pool;

/// re-export
pub use engine::KvsEngine;
pub use engine::Result;
pub use kvs_engine::KvStore;
pub use sled_engine::SledStore;
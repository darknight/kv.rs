#[macro_use]
extern crate criterion;
use std::iter;
use rand::Rng;
use rand::thread_rng;
use rand::distributions::{Alphanumeric, Uniform};

use criterion::{Criterion, ParameterizedBenchmark, Fun, BatchSize};
use criterion::black_box;
use kvs::{KvsEngine, KvStore};
use std::path::PathBuf;
use std::path::Path;
use kvs::sled_engine::SledStore;
use std::time::SystemTime;
use std::borrow::Borrow;

static BASE_PATH: &'static str = "/var/folders/sb/__xlrdmd64v3bmk86q_dg4lx8c1mtb/T/kv-bench";
static SEQ_LEN: usize = 1000;

fn generate_kv_pairs() -> Vec<(String, String)> {
    let mut rng = thread_rng();

    let mut pairs = vec![];
    for i in 0..SEQ_LEN {
        let key_len: usize = rng.gen_range(1, 100001);
        let key: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(key_len)
            .collect();

        let val_len: usize = rng.gen_range(1, 100001);
        let value: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .take(val_len)
            .collect();

        pairs.push((key, value));
    }
    pairs
}

fn generate_read_seq(pairs: &Vec<(String, String)>) -> Vec<(String, String)> {
    let mut rng = thread_rng();
    let seq: Vec<usize> = rng.sample_iter(&Uniform::new(0, SEQ_LEN))
        .take(1000)
        .collect();
    seq.iter().map(|&idx| {
        let (k, v) = pairs[idx].borrow();
        (k.to_string(), v.to_string())
    }).collect()
}

fn get_kv_store() -> KvStore {
    let base_path = Path::new(BASE_PATH);
    let full_path = base_path.join("kvs");
    KvStore::open(full_path).expect("failed to init kvs engine")
}

fn get_sled_store() -> SledStore {
    let base_path = Path::new(BASE_PATH);
    let full_path = base_path.join("sled");
    SledStore::open(full_path).expect("failed to init sled engine")
}

fn main() {
    let mut kvs = get_kv_store();
    let mut sled = get_sled_store();
    println!("create kv engine...done");

    let pairs = generate_kv_pairs();
    let mut pairs1 = vec![];
    let mut pairs2 = vec![];
    pairs1.clone_from(&pairs);
    pairs2.clone_from(&pairs);
    println!("create test data ({} pairs)...done", pairs1.len());

    let seq1 = generate_read_seq(&pairs);
    let mut seq2 = vec![];
    seq2.clone_from(&seq1);
    println!("create index sequence...done");

    let now1 = SystemTime::now();
    println!("[kvs] start testing `set`");
    for (k, v) in pairs1 {
        kvs.set(k, v);
    }
    println!("[kvs] finish testing `set`, take {}ms", now1.elapsed().unwrap().as_millis());

    let now2 = SystemTime::now();
    println!("[sled] start testing `set`");
    for (k, v) in pairs2 {
        sled.set(k, v);
    }
    println!("[sled] finish testing `set`, take {}ms", now2.elapsed().unwrap().as_millis());

    let now3 = SystemTime::now();
    println!("[kvs] start testing `get`");
    for (k, v) in seq1 {
        let target_v = kvs.get(k).unwrap().unwrap();
        assert_eq!(target_v, v);
    }
    println!("[kvs] finish testing `get`, take {}ms", now3.elapsed().unwrap().as_millis());

    let now4 = SystemTime::now();
    println!("[sled] start testing `get`");
    for (k, v) in seq2 {
        let target_v = kvs.get(k).unwrap().unwrap();
        assert_eq!(target_v, v);
    }
    println!("[sled] finish testing `get`, take {}ms", now4.elapsed().unwrap().as_millis());
}

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

static BASE_PATH: &'static str = "/var/folders/sb/__xlrdmd64v3bmk86q_dg4lx8c1mtb/T/kv-bench";
static SEQ_LEN: usize = 100;

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

fn generate_read_seq() -> Vec<usize> {
    let mut rng = thread_rng();
    let seq: Vec<usize> = rng.sample_iter(&Uniform::new(0, SEQ_LEN))
        .take(1000)
        .collect();
    seq
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

fn bench_kvs_write(c: &mut Criterion) {
    let mut kvs = get_kv_store();
    let pairs1 = generate_kv_pairs();

    c.bench_function(
        "kvs write", move |b| {
            b.iter_batched(|| {
                let mut pairs = vec![];
                pairs.clone_from(&pairs1);
                pairs
            }, |pairs| {
                for (k, v) in pairs {
                    kvs.set(k, v);
                }
            }, BatchSize::SmallInput)
        }
    );
}

fn bench_sled_write(c: &mut Criterion) {
    let mut sled = get_sled_store();
    let pairs1 = generate_kv_pairs();

    c.bench_function(
        "sled write", move |b| {
            b.iter_batched(|| {
                let mut pairs = vec![];
                pairs.clone_from(&pairs1);
                pairs
            }, |pairs| {
                for (k, v) in pairs {
                    sled.set(k, v);
                }
            }, BatchSize::SmallInput)
        }
    );
}

// FIXME: replace bin/bench.rs with this benchmark
criterion_group!(benches, bench_sled_write);
criterion_main!(benches);

extern crate rayon;

use super::*;

/// wrapper for rayon threadpool
pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {

    fn new(threads: u32) -> Result<RayonThreadPool> {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .unwrap();
        Ok(RayonThreadPool { pool })
    }

    // if job is panic, the panic will be propagated
    // which make the threadpool exit
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        self.pool.install(job)
    }
}

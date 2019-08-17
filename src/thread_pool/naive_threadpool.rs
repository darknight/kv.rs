use std::thread;

use super::*;

/// naive threadpool just fire a thread and forget, so it's just a blank struct
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {

    fn new(_: u32) -> Result<NaiveThreadPool> {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        thread::spawn(move || {
            job();
        });
    }
}

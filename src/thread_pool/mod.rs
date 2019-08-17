use super::engine::Result;

/// A simple interface for threadpool
pub trait ThreadPool {
    /// init thread pool with specified number of threads
    fn new(threads: u32) -> Result<Self> where Self: Sized;

    /// dispatch job to one of ready threads
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

/// naive threadpool
pub mod naive_threadpool;
/// shared queue threadpool;
pub mod shared_queue_threadpool;

/// re-export
pub use naive_threadpool::NaiveThreadPool;
pub use shared_queue_threadpool::SharedQueueThreadPool;


use super::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// use vec deque with mutex to provide synchronization
pub struct SharedQueueThreadPool {
    queue: Arc<Mutex<VecDeque<Job>>>,
    pool: Vec<JoinHandle<()>>,
    terminate: Arc<AtomicBool>,
}

fn queue_polling(queue: Arc<Mutex<VecDeque<Job>>>, term: Arc<AtomicBool>) {
    loop {
        if term.load(Ordering::SeqCst) {
            println!("Terminate thread of pool");
            break;
        }
        match queue.lock() {
            Ok(mut guard) => {
                let job_opt = guard.pop_front();
                if let Some(job) = job_opt {
                    // TODO: deal with panic and lock poison
                    job();
                    continue;
                }
            },
            Err(poisoned) => {
                continue
            },
        }
        // TODO: remove sleep function, use `Condvar`
        thread::sleep(Duration::from_millis(100));
    }
}

impl ThreadPool for SharedQueueThreadPool {

    fn new(threads: u32) -> Result<Self> where Self: Sized {
        let queue: Arc<Mutex<VecDeque<Job>>> = Arc::new(Mutex::new(VecDeque::new()));
        let mut pool = vec![];
        let terminate = Arc::new(AtomicBool::new(false));

        for i in 0..threads {
            let q = queue.clone();
            let term = terminate.clone();
            let t = thread::spawn(move || {
                queue_polling(q, term);
            });
            pool.push(t);
        }

        Ok(SharedQueueThreadPool {
            queue,
            pool,
            terminate,
        })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        let mut q = self.queue.lock().unwrap();
        q.push_back(Box::new(job));
    }
}

/// free threads when pool is destroyed
impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        self.terminate.store(true, Ordering::SeqCst);
        for t in self.pool.drain(..) {
            t.join();
        }
    }
}

use std::{collections::BTreeMap, sync::mpsc, thread};

// Potential improvement: instead measure how long first item takes and
// spawn multiple threads if F takes longer than, for example, 50ms
pub const DEFAULT_PARALLEL_THRESHOLD: usize = 100;

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ParallelError {
    UnableToJoin,
}

#[derive(Debug, Clone)]
pub struct ParallelMapper {
    threshold: usize,
    concurrency: usize,
}

impl Default for ParallelMapper {
    fn default() -> Self {
        Self::new()
    }
}

impl ParallelMapper {
    /// Creates new [ParallelMapper] with [DEFAULT_PARALLEL_THRESHOLD]
    /// and concurrency equal to number of system threads.
    pub fn new() -> Self {
        ParallelMapper {
            threshold: DEFAULT_PARALLEL_THRESHOLD,
            concurrency: num_cpus::get(),
        }
    }

    /// Sets custom parallelization threshold.
    pub fn with_threshold(mut self, new_threshold: usize) -> Self {
        self.threshold = new_threshold;
        self
    }

    /// Sets custom concurrency limit.
    /// # Panics
    /// Panics if `new_concurrency` is `0`.
    pub fn with_concurrency(mut self, new_concurrency: usize) -> Self {
        assert!(new_concurrency > 0);
        self.concurrency = new_concurrency;
        self
    }

    /// Maps items using provided function. Will spread work across multiple
    /// threads if number of items is greater than configured threshold.
    pub fn map<T, R, F>(&self, items: Vec<T>, f: F) -> Result<Vec<R>, ParallelError>
    where
        F: Fn(T) -> R + Clone + Send + 'static,
        T: Send + 'static,
        R: Send + 'static,
    {
        if items.len() < self.threshold {
            return Ok(items.into_iter().map(f).collect());
        }

        // since channels can mess up ordering, send item idx along the way,
        // so we can later get return results using BTreeMap
        let (results_tx, results_rx) = mpsc::channel::<(usize, R)>();

        // 1. spawn workers
        let senders: Vec<_> = (0..self.concurrency)
            .map(|_| {
                let f = f.clone();
                let results_tx = results_tx.clone();
                let (tx, rx) = mpsc::channel::<(usize, T)>();

                // threads are detached since we will rely on channels to control flow
                thread::spawn(move || {
                    for (i, t) in rx.iter() {
                        let r = f(t);
                        results_tx.send((i, r)).unwrap();
                    }
                    // results_tx dropped here, once all senders are dropped, main thread can proceed
                });

                tx
            })
            .collect();

        // 2. send actual work
        for (idx, item) in items.into_iter().enumerate() {
            senders[idx % self.concurrency].send((idx, item)).unwrap();
        }

        // drop all senders so worker threads will exit and main thread could continue
        drop(results_tx);
        drop(senders);

        // 3. collect results and returned them in sorted order
        let mut results = BTreeMap::new();

        for (i, r) in results_rx.iter() {
            results.insert(i, r);
        }

        Ok(results.into_values().collect())
    }
}

/// Calls [ParallelMapper::map] with reasonable defaults
pub fn parallel_map<T, R, F>(items: Vec<T>, f: F) -> Result<Vec<R>, ParallelError>
where
    F: Fn(T) -> R + Clone + Send + 'static,
    T: Send + 'static,
    R: Send + 'static,
{
    ParallelMapper::new().map(items, f)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn serial() {
        let data = vec![1, 2, 3];

        let f = |x| x * 2;
        let result = parallel_map(data, f);

        assert_eq!(result.unwrap(), vec![2, 4, 6]);
    }

    #[test]
    fn sleep() {
        let data = vec![
            Duration::from_millis(300),
            Duration::from_millis(300),
            Duration::from_millis(400),
            Duration::from_millis(300),
        ];

        let f = |x| std::thread::sleep(x);

        ParallelMapper::new()
            .with_threshold(0)
            .with_concurrency(4)
            .map(data, f)
            .unwrap();
    }

    #[test]
    fn parallel() {
        let data = vec![1; 150];

        let f = |x| x * 2;
        let result = parallel_map(data, f);

        assert_eq!(result.unwrap(), vec![2; 150]);
    }

    #[test]
    fn custom_threshold() {
        let data = vec![1; 150];

        let f = |x| x * 2;
        let result = ParallelMapper::new()
            .with_threshold(200)
            .with_concurrency(2)
            .map(data, f);

        assert_eq!(result.unwrap(), vec![2; 150]);
    }

    #[test]
    fn empty() {
        let data = vec![];

        let f = |x: usize| x * 2;

        let result = ParallelMapper::new()
            .with_threshold(0)
            .with_concurrency(11)
            .map(data, f);

        assert_eq!(result.unwrap(), vec![]);
    }

    #[test]
    fn shared_config() {
        use std::sync::Arc;

        let data = vec!["a".to_owned(), "bb".to_owned(), "ccc".to_owned()];

        let modifier = Arc::new(1);
        let mod_clone = Arc::clone(&modifier);

        // closure that captures external config is still possible
        let f = move |x: String| x.len() + *mod_clone;
        let result = parallel_map(data, f);

        assert_eq!(result.unwrap(), vec![2, 3, 4]);
    }
}

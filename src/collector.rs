use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use tap::Pipe;
use tokio::time::sleep;
use crate::executor::{ExecutorStats};
use crate::plan::Plan;

#[derive(Default, Clone, Debug)]
pub struct Collections {
    pub qps: Vec<u64>,
    pub concurrency: Vec<u64>,
    pub latencies_p50_ms: Vec<u64>,
    pub latencies_p90_ms: Vec<u64>,
    pub latencies_p99_ms: Vec<u64>,
}

/// Collects the statistics history
pub struct Collector<const N: usize, O>
    where
        O: Hash + Eq + Send + Clone + 'static,
{
    statistics: ExecutorStats<N, O>,
    collections: Arc<Mutex<Collections>>,
}


impl<const N: usize, O> Collector<N, O>
    where
        O: Hash + Eq + Send + Clone + 'static,
{
    pub fn new(statistics: ExecutorStats<N, O>) -> Self {
        Self {
            statistics,
            collections: Default::default(),
        }
    }

    pub fn spawn(&self) {
        let statistics = self.statistics.clone();
        let collections = self.collections.clone();

        tokio::spawn(async move {
            while let Some(frozen) = statistics.load() {
                collections.lock().pipe(|mut collections| {
                    collections.qps.push(frozen.qps.current_in_window());
                    collections.concurrency.push(frozen.concurrency.current_in_window());
                    collections.latencies_p50_ms.push(frozen.latencies.percentile(50.0).unwrap_or_default());
                    collections.latencies_p90_ms.push(frozen.latencies.percentile(90.0).unwrap_or_default());
                    collections.latencies_p99_ms.push(frozen.latencies.percentile(99.0).unwrap_or_default());
                });

                sleep(Duration::from_secs(1)).await;
            }
        });
    }

    pub fn data_view(&self) -> MutexGuard<'_, RawMutex, Collections> {
        self.collections.lock()
    }
}


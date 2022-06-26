use crate::gen::Generator;
use crate::measure::{QpsMeasure, PerSecondMeasurementState, QpsPushMetric};
use crate::plan::Plan;
use futures::{pin_mut, StreamExt};
use histogram::Histogram;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Weak};
use std::{fmt, mem};
use tap::Pipe;
use tokio::sync::Semaphore;
use tokio::time::Instant;

pub struct ExecutorStatsData<const N: usize, O>
    where
        O: Hash + Eq + Send + Clone + 'static,
{
    pub qps: QpsMeasure<N, QpsPushMetric>,
    pub concurrency: QpsMeasure<N, Histogram>,
    pub latencies: Histogram,
    pub results: HashMap<O, QpsMeasure<N, QpsPushMetric>>,
}

impl<const N: usize, O> Default for ExecutorStatsData<N, O>
    where
        O: Hash + Eq + Send + Clone + 'static,
{
    fn default() -> Self {
        ExecutorStatsData {
            qps: Default::default(),
            concurrency: Default::default(),
            latencies: Default::default(),
            results: Default::default(),
        }
    }
}


pub struct ExecutorStatsFrozenData<const N: usize, O>
    where
        O: Hash + Eq + Send + Clone + 'static,
{
    pub qps: PerSecondMeasurementState<N, QpsPushMetric>,
    pub concurrency: PerSecondMeasurementState<N, Histogram>,
    pub latencies: Histogram,
    pub results: HashMap<O, PerSecondMeasurementState<N, QpsPushMetric>>,
}

impl<const N: usize, O> fmt::Debug for ExecutorStatsFrozenData<N, O>
    where
        O: Debug + Hash + Eq + Send + Clone + 'static,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // f.write_fmt(format_args!("qps: {}\n", self.qps.current_in_window()))?;
        f.write_str("latencies:\n")?;
        f.write_fmt(format_args!(
            "\tp50 = {}\n",
            self.latencies.percentile(50.0).unwrap_or_default()
        ))?;
        f.write_fmt(format_args!(
            "\tp90 = {}\n",
            self.latencies.percentile(90.0).unwrap_or_default()
        ))?;
        f.write_fmt(format_args!(
            "\tp95 = {}\n",
            self.latencies.percentile(95.0).unwrap_or_default()
        ))?;
        f.write_fmt(format_args!(
            "\tp99 = {}\n",
            self.latencies.percentile(99.0).unwrap_or_default()
        ))?;
        f.write_fmt(format_args!(
            "\tp99.9 = {}\n",
            self.latencies.percentile(99.9).unwrap_or_default()
        ))?;
        f.write_str("results:\n")?;
        for (k, v) in &self.results {
            // f.write_fmt(format_args!("\t{:?} = {}\n", k, v.current_in_window()))?;
        }
        Ok(())
    }
}

pub struct ExecutorStats<const N: usize, O>
    where
        O: Hash + Eq + Send + Clone + 'static,
{
    inner: Weak<Mutex<ExecutorStatsData<N, O>>>,
}

impl<const N: usize, O> Clone for ExecutorStats<N, O>
    where
        O: Hash + Eq + Send + Clone + 'static,
{
    fn clone(&self) -> Self {
        ExecutorStats {
            inner: self.inner.clone(),
        }
    }
}

impl<const N: usize, O> ExecutorStats<N, O>
    where
        O: Hash + Eq + Send + Clone + 'static,
{
    pub fn load(&self) -> Option<ExecutorStatsFrozenData<N, O>> {
        if let Some(inner) = self.inner.upgrade() {
            let locked = inner.lock();

            Some(ExecutorStatsFrozenData {
                qps: locked.qps.state(),
                concurrency: locked.concurrency.state(),
                latencies: locked.latencies.clone(),
                results: locked
                    .results
                    .iter()
                    .map(|(k, v)| (k.clone(), v.state()))
                    .collect(),
            })
        } else {
            None
        }
    }
}

pub struct Executor<const N: usize, P, O, F, Fn>
    where
        P: Plan,
        O: Hash + Eq + Send + Clone + 'static,
        F: 'static + Future<Output=O> + Send,
        Fn: 'static + FnMut() -> F + Send,
{
    generator: Generator<P>,
    func: Fn,
    concurrency: Semaphore,
    concurrency_num: usize,
    stats: Arc<Mutex<ExecutorStatsData<N, O>>>,
}

impl<const N: usize, P, O, F, Fn> Executor<N, P, O, F, Fn>
    where
        P: Plan,
        O: Hash + Eq + Send + Clone + 'static,
        F: 'static + Future<Output=O> + Send,
        Fn: 'static + FnMut() -> F + Send,
{
    pub fn new(plan: P, func: Fn, max_concurrency: usize) -> Self {
        Executor {
            generator: Generator::new(plan),
            func,
            concurrency: Semaphore::new(max_concurrency),
            concurrency_num: max_concurrency,
            stats: Arc::new(Mutex::new(ExecutorStatsData::default())),
        }
    }

    pub fn stats(&self) -> ExecutorStats<N, O> {
        let weak = Arc::downgrade(&self.stats);
        ExecutorStats {
            inner: weak,
        }
    }

    pub async fn run(self) {
        let gen_stream = self.generator.start();
        let concurrency = Arc::new(self.concurrency);
        let stats = self.stats;
        let concurrency_num = self.concurrency_num;

        let mut func = self.func;

        pin_mut!(gen_stream);

        while let Some(()) = gen_stream.next().await {
            let concurrency = concurrency.clone();
            let stats = stats.clone();

            let concurrency_used = concurrency_num - concurrency.available_permits();

            stats.lock().concurrency.push_metric(concurrency_used.try_into().unwrap());

            if let Ok(permit) = concurrency.try_acquire_owned() {
                let fut = func();
                stats.lock().pipe(|locked| {
                    locked.qps.push_metric(1);
                });

                tokio::spawn(async move {
                    let started_at = Instant::now();
                    let res = fut.await;
                    let took_time = started_at.elapsed();

                    stats.lock().pipe(|mut stats| {
                        stats.results.entry(res).or_default().push_metric(1);
                        stats
                            .latencies
                            .increment(took_time.as_millis().try_into().expect("overflow"))
                            .unwrap();
                    });

                    mem::drop(permit);
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::linear::Linear;
    use rand::{thread_rng, Rng};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tokio::time::sleep;
    use crate::collector::Collector;

    #[tokio::test]
    async fn test_basic() {
        let counter = Arc::new(AtomicU64::new(0));
        let executor = Executor::<5, _, _, _, _>::new(
            Linear::new(1.0, 2.0, Duration::from_secs(2)).unwrap(),
            {
                let counter = counter.clone();

                move || {
                    let counter = counter.clone();

                    async move {
                        let to_sleep = thread_rng()
                            .gen_range(Duration::from_millis(100)..Duration::from_millis(500));
                        sleep(to_sleep).await;
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            },
            100,
        );

        let stat = executor.stats();
        let collector = Collector::new(stat);
        collector.spawn();
        executor.run().await;

        println!("collectino = {:?}", collector.data_view().clone());

        assert_eq!(counter.load(Ordering::Relaxed), 3);
    }
}

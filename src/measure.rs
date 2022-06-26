use std::mem;
use median::Filter;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use histogram::Histogram;
use tokio::select;
use tokio::sync::oneshot;
use tokio::time::{sleep, Instant};

pub trait PushMetric: Default + Clone + Send + 'static {
    fn push_metric(&mut self, metric: u64);
    fn current_value(&self) -> u64;
}

#[derive(Debug, Clone)]
pub struct PerSecondMeasurementState<const N: usize, P: PushMetric> {
    time: Instant,
    current: P,
    median: Filter<u64>,
}

impl<const N: usize, P: PushMetric> PerSecondMeasurementState<N, P> {
    pub fn current_in_window(&self) -> u64 {
        if self.median.len() == 0 {
            0
        } else {
            self.median.min()
        }
    }
}


#[derive(Clone, Copy, Debug, PartialOrd, Eq, PartialEq)]
pub struct QpsPushMetric {
    val: u64,
}

impl PushMetric for QpsPushMetric {
    fn push_metric(&mut self, metric: u64) {
        self.val += metric;
    }

    fn current_value(&self) -> u64 {
        self.val
    }
}

impl Default for QpsPushMetric {
    fn default() -> Self {
        Self { val: 0 }
    }
}

impl PushMetric for Histogram {
    fn push_metric(&mut self, metric: u64) {
        self.increment(metric).unwrap();
    }

    fn current_value(&self) -> u64 {
        self.mean().unwrap_or(0)
    }
}

pub struct QpsMeasure<const N: usize, P: PushMetric> {
    state: Arc<Mutex<PerSecondMeasurementState<N, P>>>,
    _stop_tx: oneshot::Sender<()>,
}

impl<const N: usize, P: PushMetric> Default for QpsMeasure<N, P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize, P: PushMetric> QpsMeasure<N, P> {
    pub fn new() -> Self {
        let state = Arc::new(Mutex::new(PerSecondMeasurementState {
            time: Instant::now(),
            current: Default::default(),
            median: Filter::new(N),
        }));
        let f = {
            let state = state.clone();

            async move {
                loop {
                    sleep(Duration::from_secs(1)).await;
                    let mut state = state.lock();
                    let old_val = mem::replace(&mut state.current, P::default());
                    state.median.consume(old_val.current_value());
                }
            }
        };

        let (stop_tx, stop_rx) = oneshot::channel::<()>();

        tokio::spawn(async move {
            select! {
                _ = f => {},
                _ = stop_rx => {},
            }
        });

        Self {
            state,
            _stop_tx: stop_tx,
        }
    }

    pub fn push_metric(&self, metric: u64) {
        self.state.lock().current.push_metric(metric);
    }

    pub fn current(&self) -> u64 {
        let state = self.state.lock();
        state.current.current_value()
    }

    pub fn current_in_window(&self) -> u64 {
        let state = self.state.lock();
        state.current_in_window()
    }

    pub fn state(&self) -> PerSecondMeasurementState<N, P> {
        self.state.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_measurement() {
        let measurement = QpsMeasure::<5, QpsPushMetric>::new();

        for _ in 0..2 {
            measurement.push_metric(1);
            sleep(Duration::from_millis(100)).await;
        }

        sleep(Duration::from_millis(2000)).await;

        for _ in 0..10 {
            measurement.push_metric(1);
            sleep(Duration::from_millis(10)).await;
        }
        sleep(Duration::from_millis(900)).await;

        for _ in 0..4 {
            measurement.push_metric(1);
        }

        assert_eq!(measurement.current_in_window(), 4);
        let mut median = Filter::new(5);
        median.consume(2);
        median.consume(0);
        median.consume(10);
        median.consume(4);
        median.consume(0);
        assert_eq!(measurement.current_in_window(), median.median());
    }
}

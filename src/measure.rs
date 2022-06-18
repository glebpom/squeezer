use median::Filter;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::sync::oneshot;
use tokio::time::{sleep, Instant};

#[derive(Debug, Clone)]
pub struct QpsMeasureState<const N: usize> {
    index: usize,
    time: Instant,
    slots: [u64; N],
}

impl<const N: usize> QpsMeasureState<N> {
    fn maybe_move_time_slot(&mut self) {
        let now = Instant::now();
        if now - self.time >= Duration::from_secs(1) {
            self.index = (self.index + 1) % N;
            self.slots[self.index] = 0;
            self.time = now;
        }
    }

    pub fn current(&self) -> u64 {
        self.slots[self.index]
    }

    pub fn current_in_window(&self, size: usize) -> Result<u64, ()> {
        if size > N {
            return Err(());
        }
        let mut filter = Filter::new(size);
        for n_before in (0..size).rev() {
            filter.consume(self.nth_before(n_before).expect("should never happen"));
        }
        Ok(filter.median())
    }

    fn nth_before(&self, rev_idx: usize) -> Option<u64> {
        if rev_idx > N {
            None
        } else if self.index >= rev_idx {
            Some(self.slots[self.index - rev_idx])
        } else {
            // | . . . . . | N = 5
            //       ^ idx = 2
            // rev_idx = 4 (4 slots backwar)
            // result should be 3

            Some(self.slots[self.index + N - rev_idx])
        }
    }
}

pub struct QpsMeasure<const N: usize> {
    state: Arc<Mutex<QpsMeasureState<N>>>,
    _stop_tx: oneshot::Sender<()>,
}

impl<const N: usize> Default for QpsMeasure<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> QpsMeasure<N> {
    pub fn new() -> Self {
        let state = Arc::new(Mutex::new(QpsMeasureState {
            index: 0,
            time: Instant::now(),
            slots: [0u64; N],
        }));
        let f = {
            let state = state.clone();

            async move {
                loop {
                    sleep(Duration::from_millis(100)).await;
                    let mut state = state.lock();
                    state.maybe_move_time_slot();
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

    pub fn inc(&self) {
        let mut state = self.state.lock();
        state.maybe_move_time_slot();
        let idx = state.index;
        state.slots[idx] += 1;
    }

    pub fn current(&self) -> u64 {
        let state = self.state.lock();
        state.current()
    }

    pub fn current_in_window(&self, size: usize) -> Result<u64, ()> {
        let state = self.state.lock();
        state.current_in_window(size)
    }

    pub fn state(&self) -> QpsMeasureState<N> {
        self.state.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_measurement() {
        let measurement = QpsMeasure::<5>::new();

        for _ in 0..2 {
            measurement.inc();
            sleep(Duration::from_millis(100)).await;
        }

        sleep(Duration::from_millis(2000)).await;

        for _ in 0..10 {
            measurement.inc();
            sleep(Duration::from_millis(10)).await;
        }
        sleep(Duration::from_millis(900)).await;

        for _ in 0..4 {
            measurement.inc();
        }

        assert_eq!(measurement.current(), 4);
        let mut median = Filter::new(5);
        median.consume(2);
        median.consume(0);
        median.consume(10);
        median.consume(4);
        median.consume(0);
        assert_eq!(measurement.current_in_window(5).unwrap(), median.median());
    }
}

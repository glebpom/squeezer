use crate::plan;
use crate::plan::Plan;
use futures::Future;
use futures::{ready, Stream};
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;
use tokio::time::{sleep, Sleep};

pub struct Generator<P> {
    plan: P,
}

impl<P: Plan> Generator<P> {
    pub fn new(plan: P) -> Self {
        Self { plan: plan }
    }

    pub fn start(self) -> GeneratorStream<P> {
        GeneratorStream::new(self.plan)
    }
}

enum GeneratorStreamState {
    New,
    Active {
        started_at: Instant,
        last_calculated_at: Duration,
        sleep: Pin<Box<Sleep>>,
        current_cycle_delay: Duration,
        qps: f64,
    },
}

#[pin_project]
pub struct GeneratorStream<P> {
    #[pin]
    plan: P,
    state: GeneratorStreamState,
}

impl<P: Plan> GeneratorStream<P> {
    fn new(plan: P) -> Self {
        Self {
            plan,
            state: GeneratorStreamState::New,
        }
    }
}

fn qps_to_sleep_duration(qps: f64) -> Duration {
    Duration::from_secs_f64(Duration::from_secs(1).as_secs_f64() / qps)
}

impl<P: Plan> Stream for GeneratorStream<P> {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let now = Instant::now();
        let mut this = self.project();

        if let GeneratorStreamState::New = &mut this.state {
            let qps = this
                .plan
                .qps_for(Duration::from_secs(0))
                .expect("impossible");
            let current_cycle_delay = qps_to_sleep_duration(qps);
            let sleep = sleep(current_cycle_delay);

            *this.state = GeneratorStreamState::Active {
                started_at: now,
                last_calculated_at: Duration::from_secs(0),
                sleep: Box::pin(sleep),
                current_cycle_delay,
                qps,
            };

            return Poll::Ready(Some(()));
        };

        if let GeneratorStreamState::Active {
            qps,
            started_at,
            last_calculated_at,
            sleep,
            current_cycle_delay,
        } = &mut this.state
        {
            let since_beginning = now - *started_at;

            if since_beginning - *last_calculated_at >= Duration::from_secs(1) {
                *qps = match this.plan.qps_for(since_beginning) {
                    Ok(qps) => qps,
                    Err(plan::Error::Finished) => {
                        return Poll::Ready(None);
                    }
                };
                *current_cycle_delay = qps_to_sleep_duration(*qps);
                *sleep = Box::pin(tokio::time::sleep(*current_cycle_delay));
                *last_calculated_at = since_beginning;
                return Poll::Ready(Some(()));
            }

            ready!(Pin::new(&mut (*sleep)).poll(cx));
            *sleep = Box::pin(tokio::time::sleep(*current_cycle_delay));
            return Poll::Ready(Some(()));
        }

        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::constant::Constant;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_generator() {
        let gen = Generator::new(Constant::new(1.0, Duration::from_secs(10)).unwrap());
        let mut events = gen.start();
        let started_at = Instant::now();
        let mut counter = 0;
        while let Some(()) = events.next().await {
            counter += 1;
        }
        let took_time = started_at.elapsed();
        assert_eq!(took_time.as_secs(), 10);
        assert_eq!(counter, 10);
    }
}

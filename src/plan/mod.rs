use std::time::Duration;

pub mod constant;
pub mod linear;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("plan execution is already finished at this time")]
    Finished,
}

pub trait Plan {
    /// Calculates the number of events per seconds for the desired time
    ///
    /// since_beginning - time passed since the beginning of the plan execution
    fn qps_for(&self, since_beginning: Duration) -> Result<f64, Error>;
}

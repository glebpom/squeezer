use crate::plan::{Error, Plan};
use std::time::Duration;

pub struct Linear {
    from: f64,
    to: f64,
    period: Duration,
}

impl Plan for Linear {
    fn qps_for(&self, since_beginning: Duration) -> Result<f64, Error> {
        if since_beginning > self.period {
            return Err(Error::Finished);
        }

        Ok(
            ((self.to - self.from) / self.period.as_secs_f64()) * since_beginning.as_secs_f64()
                + self.from,
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LinearError {
    #[error("bad input parameters")]
    BadInputParams,
}

impl Linear {
    pub fn new(from: f64, to: f64, period: Duration) -> Result<Self, LinearError> {
        if !from.is_sign_positive()
            || !from.is_sign_positive()
            || !period.as_secs_f64().is_sign_positive()
        {
            return Err(LinearError::BadInputParams);
        }

        Ok(Linear { from, to, period })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear() {
        let linear = Linear::new(10.0, 1000.0, Duration::from_secs(60)).unwrap();
        assert_eq!(linear.qps_for(Duration::from_secs(0)).unwrap(), 10.0);
        assert_eq!(linear.qps_for(Duration::from_secs(30)).unwrap(), 505.0);
        assert_eq!(linear.qps_for(Duration::from_secs(60)).unwrap(), 1000.0);
    }
}

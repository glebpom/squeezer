use crate::plan::{Error, Plan};
use std::time::Duration;

pub struct Constant {
    val: f64,
    period: Duration,
}

impl Plan for Constant {
    fn qps_for(&self, since_beginning: Duration) -> Result<f64, Error> {
        if since_beginning > self.period {
            return Err(Error::Finished);
        }

        Ok(self.val)
    }
}

impl Constant {
    pub fn new(val: f64, period: Duration) -> Result<Self, ()> {
        Ok(Constant { val, period })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant() {
        let constant = Constant::new(10.0, Duration::from_secs(60)).unwrap();
        assert_eq!(constant.qps_for(Duration::from_secs(0)).unwrap(), 10.0);
    }
}

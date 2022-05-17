use std::marker::PhantomData;
use crate::error::Result;

struct Bin {
    id: usize,
    from: f64,
    to: f64,
}

enum Interpolation {
    Linear,
    Bicubic,
}

pub struct Histogram<T> {
    interpolation: PhantomData<T>,
    points: usize,
    avg_points_per_bucket: f64,
}

pub struct Linear {
    len: usize,
}

impl Linear {
    pub fn try_new(len: usize, buckets: &[f64]) -> Result<Self> {
        assert!(len > 0);
        assert!(buckets.len() > 0);

        unimplemented!()
        /*Self {
            len,

        }*/
    }
    pub fn probability_at_x(x: f64) -> f64 {
        unimplemented!()
    }
}
use chrono::{DateTime, Utc};
use rand::rngs::ThreadRng;

pub struct Generator {
    rng: ThreadRng,
    cur_timestamp: i64,
    to: i64,
    uniques_left: usize,
    peak_uniq_per_second: usize,
    closed: bool,
}

pub struct Sample {}

impl Generator {
    pub fn new(rng: ThreadRng, from: DateTime<Utc>, to: DateTime<Utc>, peak_uniq_per_second: usize) -> Self {
        assert!(to > from);
        Self {
            rng,
            cur_timestamp: from.timestamp(),
            to: to.timestamp(),
            uniques_left: peak_uniq_per_second,
            peak_uniq_per_second,
            closed: false,
        }
    }

    pub fn next(&mut self) -> Option<Sample> {
        self.uniques_left -= 1;
        if self.uniques_left == 0 {
            if self.cur_timestamp == self.to {
                self.closed = true;
                return None;
            }
            self.to += 1;
            self.uniques_left = self.peak_uniq_per_second;
        }
        Some(Sample {})
    }
}
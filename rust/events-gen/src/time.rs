use chrono::{DateTime, Duration, Utc};
use rand::rngs::ThreadRng;
use rand::prelude::*;

pub struct Time {
    pub timestamp: i64,
}

impl Time {
    pub fn new(timestamp: i64) -> Self {
        Self {
            timestamp,
        }
    }

    pub fn wait_between(&mut self, from: u64, to: u64, rng: &mut ThreadRng) {
        let wait = Duration::seconds(rng.gen_range(from..=to));
        self.timestamp += wait.num_seconds();
    }
}
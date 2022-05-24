use chrono::{DateTime, Duration, Utc};
use rand::distributions::WeightedIndex;
use rand::rngs::ThreadRng;
use rand::prelude::*;

pub struct Generator {
    rng: ThreadRng,
    cur_timestamp: i64,
    to_timestamp: i64,
    daily_users_left: usize,
    new_daily_users: usize,
    total_users: usize,
    traffic_hourly_weight_idx: WeightedIndex<f64>,
    closed: bool,
}

pub struct Sample {
    pub cur_timestamp: i64,
}

impl Generator {
    pub fn new(rng: ThreadRng, from: DateTime<Utc>, to: DateTime<Utc>, new_daily_users: usize, traffic_hourly_weights: &[f64; 24]) -> Self {
        assert!(to > from);

        Self {
            rng,
            cur_timestamp: from.timestamp(),
            to_timestamp: to.timestamp(),
            traffic_hourly_weight_idx: WeightedIndex::new(traffic_hourly_weights).unwrap(),
            daily_users_left: new_daily_users,
            new_daily_users,
            total_users: 0,
            closed: false,
        }
    }

    pub fn next(&mut self) -> Option<Sample> {
        let hour = self.traffic_hourly_weight_idx.sample(&mut self.rng) as i64;
        let minute: i64 = self.rng.gen_range(0..=59);
        let sample = Sample {
            cur_timestamp: self.cur_timestamp + (hour * 3600) + minute * 60,
        };
        self.total_users += 1;
        self.daily_users_left -= 1;
        if self.daily_users_left == 0 {
            if self.cur_timestamp + 3600 * 24 >= self.to_timestamp {
                return None;
            }

            self.daily_users_left = self.new_daily_users;
            self.cur_timestamp += 3600 * 24;
            println!("generated users: {}. {} days left", self.total_users, Duration::seconds(self.to_timestamp - self.cur_timestamp).num_days())
        }

        Some(sample)
    }
}
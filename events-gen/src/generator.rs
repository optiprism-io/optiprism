use crate::profiles::{Profile, ProfileProvider};
use chrono::{DateTime, Duration, Utc};
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand::rngs::ThreadRng;

pub struct Generator {
    rng: ThreadRng,
    profiles: ProfileProvider,
    cur_timestamp: i64,
    to_timestamp: i64,
    daily_users_left: usize,
    new_daily_users: usize,
    total_users: usize,
    traffic_hourly_weight_idx: WeightedIndex<f64>,
}

pub struct Sample {
    pub cur_timestamp: i64,
    pub profile: Profile,
}

pub struct Config {
    pub rng: ThreadRng,
    pub profiles: ProfileProvider,
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub new_daily_users: usize,
    pub traffic_hourly_weights: [f64; 24],
}

impl Generator {
    pub fn new(cfg: Config) -> Self {
        assert!(cfg.from < cfg.to);

        Self {
            rng: cfg.rng,
            profiles: cfg.profiles,
            cur_timestamp: cfg.from.timestamp(),
            to_timestamp: cfg.to.timestamp(),
            traffic_hourly_weight_idx: WeightedIndex::new(cfg.traffic_hourly_weights).unwrap(),
            daily_users_left: cfg.new_daily_users,
            new_daily_users: cfg.new_daily_users,
            total_users: 0,
        }
    }

    pub fn next(&mut self) -> Option<Sample> {
        let hour = self.traffic_hourly_weight_idx.sample(&mut self.rng) as i64;
        let minute: i64 = self.rng.gen_range(0..=59);
        let sample = Sample {
            cur_timestamp: self.cur_timestamp + (hour * 3600) + minute * 60,
            profile: self.profiles.sample(&mut self.rng),
        };
        self.total_users += 1;
        self.daily_users_left -= 1;
        if self.daily_users_left == 0 {
            if self.cur_timestamp + 3600 * 24 >= self.to_timestamp {
                return None;
            }

            self.daily_users_left = self.new_daily_users;
            self.cur_timestamp += 3600 * 24;
            println!(
                "generated users: {}. {} days left",
                self.total_users,
                Duration::seconds(self.to_timestamp - self.cur_timestamp).num_days()
            )
        }

        Some(sample)
    }
}

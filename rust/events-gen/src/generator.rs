use crate::profiles::{Profile, ProfileProvider};
use chrono::{DateTime, Duration, Utc};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand::rngs::ThreadRng;
use std::fmt::Write;

pub struct Generator {
    rng: ThreadRng,
    profiles: ProfileProvider,
    // i64 is more performant for timestamp than calculations between DateTime and Duration
    cur_timestamp: i64,
    to_timestamp: i64,
    daily_users_left: usize,
    new_daily_users: usize,
    total_users: usize,
    traffic_hourly_weight_idx: WeightedIndex<f64>,
    pb: ProgressBar,
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

        let pb = ProgressBar::new((cfg.to - cfg.from).num_days() as u64);

        pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} days ({eta})")
            .unwrap()
            .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
            .progress_chars("#>-"));
        Self {
            rng: cfg.rng,
            profiles: cfg.profiles,
            cur_timestamp: cfg.from.timestamp(),
            to_timestamp: cfg.to.timestamp(),
            traffic_hourly_weight_idx: WeightedIndex::new(cfg.traffic_hourly_weights).unwrap(),
            daily_users_left: cfg.new_daily_users,
            new_daily_users: cfg.new_daily_users,
            total_users: 0,
            pb,
        }
    }

    pub fn next_sample(&mut self) -> Option<Sample> {
        let hour = self.traffic_hourly_weight_idx.sample(&mut self.rng) as i64;
        let minute: i64 = self.rng.gen_range(0..=59);
        let sample = Sample {
            cur_timestamp: self.cur_timestamp
                + (Duration::hours(hour).num_seconds())
                + Duration::minutes(minute).num_seconds(),
            profile: self.profiles.sample(&mut self.rng),
        };
        self.total_users += 1;
        self.daily_users_left -= 1;
        if self.daily_users_left == 0 {
            // look forward for a new day
            if self.cur_timestamp + Duration::days(1).num_seconds() >= self.to_timestamp {
                // and generation on the last day
                self.pb.finish_with_message("done");

                return None;
            }

            self.daily_users_left = self.new_daily_users;
            self.cur_timestamp += Duration::days(1).num_seconds();

            self.pb.inc(1);

            /*debug!(
                "generated users: {}. {} days left",
                self.total_users,
                Duration::seconds(self.to_timestamp - self.cur_timestamp).num_days()
            )*/
        }

        Some(sample)
    }
}

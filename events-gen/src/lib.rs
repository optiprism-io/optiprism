#[macro_use]
extern crate crossbeam_channel;

pub mod error;
pub mod generator;
pub mod probability;
pub mod profiles;
pub mod store;
// pub mod time;

use cubic_spline::TryFrom;

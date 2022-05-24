#[macro_use]
extern crate crossbeam_channel;

pub mod store;
pub mod error;
pub mod probability;
pub mod generator;
pub mod profile;
// pub mod time;

use cubic_spline::TryFrom;


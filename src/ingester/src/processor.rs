use std::fmt::Debug;

use crate::error::Result;
use crate::track;
use crate::track::Track;
use crate::Context;

pub trait Processor: Send + Sync {
    fn track(&self, ctx: &Context, track: Track) -> Result<Track>;
}

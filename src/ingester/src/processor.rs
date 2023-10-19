use std::fmt::Debug;

use crate::error::Result;
use crate::track;
use crate::track::Track;
use crate::AppContext;

pub trait Processor: Send + Sync {
    fn track(&self, ctx: &AppContext, track: Track) -> Result<Track>;
}

use crate::error::Result;
use crate::track;
use crate::track::Track;
use crate::Context;

pub trait Processor {
    fn track(&mut self, ctx: &Context, track: Track) -> Result<Track>;
}

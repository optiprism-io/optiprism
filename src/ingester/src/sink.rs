use crate::error::Result;
use crate::track;
use crate::Context;

pub trait Sink {
    fn track(&self, ctx: &Context, track: track::Track) -> Result<()>;
}

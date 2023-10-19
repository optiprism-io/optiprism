use crate::error::Result;
use crate::track;
use crate::AppContext;

pub trait Destination: Send + Sync {
    fn track(&self, ctx: &AppContext, track: track::Track) -> Result<()>;
}

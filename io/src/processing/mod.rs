pub mod device;
pub mod geo;
pub mod provider;
pub mod udfs;

pub use provider::Provider;

use crate::Result;
use metadata::events_queue::{Context, Event};

trait Processor {
    fn process(&self, context: &Option<Context>, event: &Event) -> Result<Option<Event>>;
}

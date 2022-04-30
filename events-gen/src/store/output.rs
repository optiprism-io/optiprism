use std::fmt::Debug;
use crate::session::{EventRecord, OutputWriter};
use crate::store::props::{EventProperties, UserProperties};
use crate::error::{Error, Result};

pub struct PrintOutput {}

impl OutputWriter<EventProperties, UserProperties> for PrintOutput {
    fn write(&mut self, event: &EventRecord<EventProperties>, user_props: &UserProperties) -> Result<()> {
        println!("{:?}", event);
        println!("{:?}", user_props);
        Ok(())
    }
}
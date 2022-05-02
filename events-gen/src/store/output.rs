use std::fmt::Debug;
use crate::session::{EventRecord, OutputWriter, Property};
use crate::error::{Error, Result};

pub struct PrintOutput {}

impl OutputWriter for PrintOutput {
    fn write(&mut self, event: &EventRecord, user_props: &Option<Vec<Property>>) -> Result<()> {
        println!("{:?}", event);
        println!("{:?}", user_props);
        Ok(())
    }
}
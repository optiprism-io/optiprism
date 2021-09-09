use crate::storage::storage::Row;
use super::error::Result;

pub struct Log {}

impl Log {
    pub fn write(&mut self, row: &Row) -> Result<()> {
        Ok(())
    }
}
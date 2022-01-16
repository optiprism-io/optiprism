use crate::exprtree::error::{Error, Result};

pub struct Record {
    id: u64,
    name: String,
}

trait DictionaryProvider {
    fn get_by_name_or_create(&self, dict: String, name: String) -> Result<Record>;
    // todo how to handle record not found response? With Option or with Err?
    fn get_by_name(&self, dict: String, name: String) -> Result<Record>;
    fn get_by_id(&self, dict: String, id: u64) -> Result<Record>;
}

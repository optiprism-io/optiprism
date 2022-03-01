use crate::dictionaries::Provider;
use crate::dictionaries::provider::ID;
use crate::error::Result;

pub struct MockDictionary {
    pub get_id_by_key: Option<fn(table: &str, key: &str) -> Result<ID>>,
    pub get_key_by_id: Option<fn(table: &str, id: ID) -> Result<String>>,
}

impl MockDictionary {
    pub fn new() -> Self {
        MockDictionary {
            get_id_by_key: None,
            get_key_by_id: None,
        }
    }
}

impl Provider for MockDictionary {
    fn get_id_by_key(&self, table: &str, key: &str) -> crate::Result<ID> {
        self.get_id_by_key.unwrap()(table, key)
    }

    fn get_key_by_id(&self, table: &str, id: ID) -> crate::Result<String> {
        self.get_key_by_id.unwrap()(table, id)
    }
}
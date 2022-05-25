use std::sync::Arc;
use crate::Store;
use crate::error::Result;

pub struct Provider {
    store: Arc<Store>,
}

impl Provider {
    pub fn new() -> Self {
        Self {}
    }

    pub async fn get_key_or_create(self, dict: String, value: String) -> Result<u64> {
        Ok(())
    }

    pub async fn get_value(&self, dict: String, key: u64) -> &String {
        self.key_values[dict as usize].get(&key).unwrap()
    }
}
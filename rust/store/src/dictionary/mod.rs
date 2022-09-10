use super::error::Result;

pub struct MockDictionary {
    pub get_u8_by_key: Option<fn(table: &str, key: &str) -> Result<u8>>,
    pub get_u16_by_key: Option<fn(table: &str, key: &str) -> Result<u16>>,
    pub get_u32_by_key: Option<fn(table: &str, key: &str) -> Result<u32>>,
    pub get_u64_by_key: Option<fn(table: &str, key: &str) -> Result<u64>>,
    pub get_key_by_u8: Option<fn(table: &str, value: u8) -> Result<String>>,
    pub get_key_by_u16: Option<fn(table: &str, value: u16) -> Result<String>>,
    pub get_key_by_u32: Option<fn(table: &str, value: u32) -> Result<String>>,
    pub get_key_by_64: Option<fn(table: &str, value: u64) -> Result<String>>,
}

impl MockDictionary {
    pub fn new() -> Self {
        MockDictionary {
            get_u8_by_key: None,
            get_u16_by_key: None,
            get_u32_by_key: None,
            get_u64_by_key: None,
            get_key_by_u8: None,
            get_key_by_u16: None,
            get_key_by_u32: None,
            get_key_by_64: None,
        }
    }
}

impl DictionaryProvider for MockDictionary {
    fn get_u8_by_key(&self, table: &str, key: &str) -> Result<u8> {
        self.get_u8_by_key.unwrap()(table, key)
    }

    fn get_u16_by_key(&self, table: &str, key: &str) -> Result<u16> {
        self.get_u16_by_key.unwrap()(table, key)
    }

    fn get_u32_by_key(&self, table: &str, key: &str) -> Result<u32> {
        self.get_u32_by_key.unwrap()(table, key)
    }

    fn get_u64_by_key(&self, table: &str, key: &str) -> Result<u64> {
        self.get_u64_by_key.unwrap()(table, key)
    }

    fn get_key_by_u8(&self, table: &str, value: u8) -> Result<String> {
        self.get_key_by_u8.unwrap()(table, value)
    }

    fn get_key_by_u16(&self, table: &str, value: u16) -> Result<String> {
        self.get_key_by_u16.unwrap()(table, value)
    }

    fn get_key_by_u32(&self, table: &str, value: u32) -> Result<String> {
        self.get_key_by_u32.unwrap()(table, value)
    }

    fn get_key_by_u64(&self, table: &str, value: u64) -> Result<String> {
        self.get_key_by_64.unwrap()(table, value)
    }
}

pub trait DictionaryProvider {
    fn get_u8_by_key(&self, table: &str, key: &str) -> Result<u8>;
    fn get_u16_by_key(&self, table: &str, key: &str) -> Result<u16>;
    fn get_u32_by_key(&self, table: &str, key: &str) -> Result<u32>;
    fn get_u64_by_key(&self, table: &str, key: &str) -> Result<u64>;

    fn get_key_by_u8(&self, table: &str, value: u8) -> Result<String>;
    fn get_key_by_u16(&self, table: &str, value: u16) -> Result<String>;
    fn get_key_by_u32(&self, table: &str, value: u32) -> Result<String>;
    fn get_key_by_u64(&self, table: &str, value: u64) -> Result<String>;
}

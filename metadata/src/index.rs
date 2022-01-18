pub trait Index {
    fn key_value_pairs(&self) -> Vec<Option<(Vec<u8>, Vec<u8>)>>;
}
use serde::Serialize;

#[derive(Serialize)]
pub struct List<T> {
    pub data: Vec<T>,
    pub total: u64,
}

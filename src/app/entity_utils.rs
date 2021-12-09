use serde::Serialize;

#[derive(Serialize)]
pub struct List<T: Serialize> {
    pub data: Vec<T>,
    pub total: u64,
}

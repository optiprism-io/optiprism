use arrow::array::BooleanArray;
use chrono::{DateTime, Utc};
use crate::error::QueryError::Plan;

#[derive(Debug, Clone)]
pub struct Step {
    pub ts: i64,
    pub row_id: usize,
    pub exists: BooleanArray,
    pub is_completed: bool,
}

pub struct FunnelResult {
    pub steps: Vec<Step>,
    pub last_step: usize,
    pub is_completed: bool,
}


use chrono::DateTime;
use chrono::Utc;

#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub enum Format {
    #[default]
    Regular,
    Compact,
}

#[derive(Default, Clone)]
pub struct Context {
    pub project_id: u64,
    pub format: Format,
    pub cur_time: DateTime<Utc>,
}

impl Context {
    pub fn new(project_id: u64) -> Self {
        Self {
            project_id,
            ..Default::default()
        }
    }
}

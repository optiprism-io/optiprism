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
    pub organization_id: u64,
    pub project_id: u64,
    pub format: Format,
    pub cur_time: DateTime<Utc>,
}

impl Context {
    pub fn new(organization_id: u64, project_id: u64) -> Self {
        Self {
            organization_id,
            project_id,
            ..Default::default()
        }
    }
}

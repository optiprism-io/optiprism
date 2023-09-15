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

#[derive(Default)]
pub struct Context {
    pub organization_id: u64,
    pub account_id: u64,
    pub project_id: u64,
}

impl Context {
    pub fn new(organization_id: u64, account_id: u64, project_id: u64) -> Self {
        Self {
            organization_id,
            account_id,
            project_id,
        }
    }
}
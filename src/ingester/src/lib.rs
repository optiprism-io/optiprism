mod error;
pub mod executor;
mod processor;
mod processors;
mod sink;
mod sources;
mod track;

pub struct Context {
    project_id: u64,
    organization_id: u64,
}

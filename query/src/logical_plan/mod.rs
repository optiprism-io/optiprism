use std::sync::Arc;
use datafusion_common::DFSchemaRef;
use crate::logical_plan::plan::LogicalPlan;

pub mod expr;
pub mod nodes;
pub mod plan;
pub mod scan_multiplexer;

/*pub struct ScanMultiplexerNode {
    input: Arc<LogicalPlan>,
    df_input: Arc<DFLogicalPlan>,
    schema: DFSchemaRef,
    readers: usize,
}*/
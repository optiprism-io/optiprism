use std::sync::Arc;

use crate::error::Result;
use crate::logical_plan::funnel::FunnelNode;
use crate::physical_plan::funnel::FunnelFinalExec;
use crate::physical_plan::funnel::FunnelPartialExec;
pub(crate) fn funnel(node: &FunnelNode) -> Result<FunnelFinalExec> {
    let partial = FunnelPartialExec::try_new()?;

    Ok(FunnelFinalExec::try_new(Arc::new(partial))?)
}

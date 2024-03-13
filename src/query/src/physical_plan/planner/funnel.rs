use std::sync::Arc;
use crate::logical_plan::funnel::FunnelNode;
use crate::physical_plan::funnel::{FunnelFinalExec, FunnelPartialExec};
use crate::error::Result;
pub(crate) fn funnel(node: &FunnelNode) ->Result<FunnelFinalExec> {
    let partial = FunnelPartialExec::try_new()?;

    Ok(FunnelFinalExec::try_new(Arc::new(partial))?)
}

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::query::event_segmentation::Segment;
use common::query::event_segmentation::SegmentCondition;
use common::query::PropertyRef;
use datafusion_common::Column;
use datafusion_expr::LogicalPlan;
use futures::executor::block_on;
use metadata::MetadataProvider;

use crate::error::Result;
use crate::expr::property_col;
use crate::expr::property_expression;
use crate::logical_plan::segmentation::SegmentationExpr;
use crate::logical_plan::segmentation::SegmentationNode;
use crate::Context;

pub(crate) fn build_segments_logical_plan(
    ctx: &Context,
    md: &Arc<MetadataProvider>,
    input: LogicalPlan,
    segments: &Vec<Segment>,
    cur_time: DateTime<Utc>,
    partition_columns: Vec<Column>,
) -> Result<LogicalPlan> {
    let exprs: Vec<SegmentationExpr> = Vec::with_capacity(segments.len());
    for segment in segments {
        for condition in &segment.conditions {
            match condition {
                SegmentCondition::HasPropertyValue {
                    property_name,
                    operation,
                    value,
                } => {
                    let prop_ref = PropertyRef::User(property_name.to_owned());
                    let prop_expr = block_on(property_expression(
                        ctx,
                        md,
                        &prop_ref,
                        operation,
                        value.to_owned(),
                    ))?;

                    let property = block_on(property_col(ctx, md, &prop_ref))?;
                }
                SegmentCondition::HadPropertyValue { .. } => {}
                SegmentCondition::DidEvent { .. } => {}
            }
        }
    }

    unimplemented!()
}

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::query::event_segmentation::DidEventAggregate;
use common::query::event_segmentation::Segment;
use common::query::event_segmentation::SegmentCondition;
use common::query::event_segmentation::SegmentTime;
use common::query::PropValueOperation;
use common::query::PropertyRef;
use common::query::TimeIntervalUnit;
use datafusion_common::Column;
use datafusion_common::ScalarValue;
use datafusion_expr::and;
use datafusion_expr::binary_expr;
use datafusion_expr::col;
use datafusion_expr::lit;
use datafusion_expr::or;
use datafusion_expr::LogicalPlan;
use datafusion_expr::Operator;
use futures::executor::block_on;
use metadata::MetadataProvider;

use crate::error::Result;
use crate::event_fields;
use crate::expr::event_expression;
use crate::expr::event_filters_expression;
use crate::expr::property_col;
use crate::expr::property_expression;
use crate::logical_plan::segmentation::AggregateFunction;
use crate::logical_plan::segmentation::SegmentationExpr;
use crate::logical_plan::segmentation::SegmentationNode;
use crate::logical_plan::segmentation::TimeRange;
use crate::physical_plan::expressions::segmentation::comparison;
use crate::Context;

pub(crate) fn build_segments_logical_plan(
    ctx: &Context,
    md: &Arc<MetadataProvider>,
    input: LogicalPlan,
    segments: &Vec<Segment>,
    cur_time: DateTime<Utc>,
    partition_columns: Vec<Column>,
) -> Result<LogicalPlan> {
    for segment in segments {
        let or_conditions: Vec<Vec<SegmentationExpr>> = Vec::new();
        let and_conditions: Vec<SegmentationExpr> = Vec::new();
        for or_conditions in &segment.conditions {
            for condition in or_conditions {
                match condition {
                    SegmentCondition::HasPropertyValue { .. } => {}
                    SegmentCondition::HadPropertyValue {
                        property_name,
                        operation,
                        value,
                        time,
                    } => {
                        let filter = block_on(property_expression(
                            ctx,
                            md,
                            &PropertyRef::User(property_name.to_owned()),
                            operation,
                            value.to_owned(),
                        ))?;
                        let time_window = if let SegmentTime::Each { n, unit } = time {
                            Some(unit.duration(*n).num_milliseconds())
                        } else {
                            None
                        };

                        let time_range = match time {
                            SegmentTime::Between { from, to } => {
                                TimeRange::Between(from.timestamp_millis(), to.timestamp_millis())
                            }
                            SegmentTime::From(from) => TimeRange::From(from.timestamp_millis()),
                            SegmentTime::Last { n, unit } => {
                                TimeRange::Last(*n, unit.duration(*n).num_milliseconds())
                            }
                            _ => {}
                        };

                        let left = SegmentationExpr {
                            filter,
                            agg_fn: AggregateFunction::Count,
                            time_range,
                            time_window,
                        };
                        Comparison::Gte(left, ScalarValue::Int64(Some(1)))
                    }
                    SegmentCondition::DidEvent {
                        event,
                        filters,
                        aggregate,
                    } => {
                        // event expression
                        let mut expr = and(expr, block_on(event_expression(ctx, md, &event))?);
                        // apply event filters
                        if let Some(filters) = &filters {
                            expr = and(
                                expr.clone(),
                                block_on(event_filters_expression(ctx, md, filters))?,
                            )
                        }

                        match aggregate {
                            DidEventAggregate::Count {
                                operation,
                                value,
                                time,
                            } => {
                                let time_range = match time {
                                    // todo make common
                                    SegmentTime::Between { from, to } => TimeRange::Between(
                                        from.timestamp_millis(),
                                        to.timestamp_millis(),
                                    ),
                                    SegmentTime::From(from) => {
                                        TimeRange::From(from.timestamp_millis())
                                    }
                                    SegmentTime::Last { n, unit } => {
                                        TimeRange::Last(*n, unit.duration(*n).num_milliseconds())
                                    }
                                    SegmentTime::Each { n, unit } => {
                                        TimeRange::Each(unit.duration(*n).num_milliseconds())
                                    }
                                    _ => unimplemented!(),
                                };

                                let left = Arc::new(SegmentationExpr {
                                    filter: expr,
                                    agg_fn: AggregateFunction::Count,
                                    time_range,
                                });
                                let op = match operation {
                                    PropValueOperation::Eq => {
                                        comparison::Eq::new(left, ScalarValue::from(*value as i64))
                                    }
                                    _ => unimplemented!("implement me!"),
                                };
                            }
                            DidEventAggregate::RelativeCount { .. } => {}
                            DidEventAggregate::AggregateProperty { .. } => {}
                            DidEventAggregate::HistoricalCount { .. } => {}
                        }
                    }
                }
            }
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

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::query::event_segmentation::DidEventAggregate;
use common::query::event_segmentation::QueryAggregate;
use common::query::event_segmentation::Segment;
use common::query::event_segmentation::SegmentCondition;
use common::query::event_segmentation::SegmentTime;
use common::query::Operator;
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
use datafusion_expr::Extension;
use datafusion_expr::LogicalPlan;
use futures::executor;
use futures::executor::block_on;
use metadata::MetadataProvider;

use crate::error::Result;
use crate::event_fields;
use crate::expr::event_expression;
use crate::expr::event_filters_expression;
use crate::expr::property_col;
use crate::expr::property_expression;
use crate::logical_plan::segmentation::Agg;
use crate::logical_plan::segmentation::AggregateFunction;
use crate::logical_plan::segmentation::SegmentationExpr;
use crate::logical_plan::segmentation::SegmentationNode;
use crate::logical_plan::segmentation::TimeRange;
use crate::physical_plan::expressions::segmentation::comparison;
use crate::physical_plan::expressions::segmentation::comparison::And;
use crate::Context;

pub(crate) fn build_segments_logical_plan(
    ctx: &Context,
    md: &Arc<MetadataProvider>,
    input: LogicalPlan,
    segments: &Vec<Segment>,
    cur_time: DateTime<Utc>,
    partition_columns: Vec<Column>,
    ts_col: Column,
) -> Result<LogicalPlan> {
    for segment in segments {
        let mut exprs: Vec<Vec<SegmentationExpr>> = Vec::new();
        for conditions in &segment.conditions {
            let mut and_conditions: Vec<SegmentationExpr> = Vec::new();
            for and_condition in conditions {
                let expr = match and_condition {
                    SegmentCondition::HasPropertyValue { .. } => unimplemented!(),
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

                        let time_range = TimeRange::from_segment_time(time.to_owned(), cur_time);

                        SegmentationExpr {
                            filter,
                            agg_fn: AggregateFunction::Count {
                                op: Operator::GtEq,
                                right: 1,
                            },
                            time_range,
                            time_window,
                        }
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
                                let time_window = if let SegmentTime::Each { n, unit } = time {
                                    Some(unit.duration(*n).num_milliseconds())
                                } else {
                                    None
                                };
                                let time_range =
                                    TimeRange::from_segment_time(time.to_owned(), cur_time);

                                SegmentationExpr {
                                    filter: expr,
                                    agg_fn: AggregateFunction::Count {
                                        op: operation.into(),
                                        right: *value as i64,
                                    },
                                    time_range,
                                    time_window,
                                }
                            }
                            DidEventAggregate::RelativeCount { .. } => unimplemented!(),
                            DidEventAggregate::AggregateProperty {
                                property,
                                aggregate,
                                operation,
                                value,
                                time,
                            } => {
                                let time_window = if let SegmentTime::Each { n, unit } = time {
                                    Some(unit.duration(*n).num_milliseconds())
                                } else {
                                    None
                                };
                                let time_range =
                                    TimeRange::from_segment_time(time.to_owned(), cur_time);
                                let agg = Agg {
                                    left: Column::new_unqualified(property.name()),
                                    op: operation.into(),
                                    right: value.unwrap().to_owned(),
                                };

                                let agg_fn = match aggregate {
                                    QueryAggregate::Min => AggregateFunction::Min(agg),
                                    QueryAggregate::Max => AggregateFunction::Max(agg),
                                    QueryAggregate::Sum => AggregateFunction::Sum(agg),
                                    QueryAggregate::Avg => AggregateFunction::Avg(agg),
                                    _ => unimplemented!(),
                                };

                                SegmentationExpr {
                                    filter: expr,
                                    agg_fn,
                                    time_range,
                                    time_window,
                                }
                            }
                            DidEventAggregate::HistoricalCount { .. } => unimplemented!(),
                        }
                    }
                };
                and_conditions.push(expr);
            }

            exprs.push(and_conditions)
        }
    }

    let node = SegmentationNode::try_new(input, ts_col, partition_columns, exprs)?;
    let plan = LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    });

    Ok(plan)
}

use std::ops::Sub;
use std::sync::Arc;
use chrono::{DateTime, Utc};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{col, Expr, lit, Operator};
use datafusion_expr::expr_fn::{and, binary_expr};
use metadata::Metadata;
use metadata::properties::provider::Namespace;
use crate::{Context, Error, event_fields};
use crate::logical_plan::expr::{lit_timestamp, multi_or};
use crate::reports::types::{PropertyRef, PropValueOperation, QueryTime};
use crate::Result;

/// builds expression on timestamp
pub fn time_expression(time: &QueryTime, cur_time: &DateTime<Utc>) -> Expr {
    let ts_col = Expr::Column(Column::from_qualified_name(event_fields::CREATED_AT));
    match time {
        QueryTime::Between { from, to } => {
            let from = binary_expr(
                ts_col.clone(),
                Operator::GtEq,
                lit_timestamp(from.timestamp_nanos() / 1_000),
            );

            let to = binary_expr(
                ts_col,
                Operator::LtEq,
                lit_timestamp(to.timestamp_nanos() / 1_000),
            );

            and(from, to)
        }
        QueryTime::From(from) => {
            let from = binary_expr(
                ts_col.clone(),
                Operator::GtEq,
                lit_timestamp(from.timestamp_nanos() / 1_000));

            let to = binary_expr(
                ts_col,
                Operator::LtEq,
                lit_timestamp(cur_time.timestamp_nanos() / 1_000),
            );

            and(from, to)
        }
        QueryTime::Last { last, unit } => {
            let from_time = cur_time.sub(unit.duration(*last));
            let from = binary_expr(
                ts_col.clone(),
                Operator::GtEq,
                lit_timestamp(from_time.timestamp_nanos() / 1_000));

            let to = binary_expr(
                ts_col,
                Operator::LtEq,
                lit_timestamp(cur_time.timestamp_nanos() / 1_000),
            );

            and(from, to)
        }
    }
}

/// builds name [property] [op] [value] expression
pub async fn property_expression(
    ctx: &Context,
    md: &Arc<Metadata>,
    property: &PropertyRef,
    operation: &PropValueOperation,
    value: Option<Vec<ScalarValue>>,
) -> Result<Expr> {
    match property {
        PropertyRef::User(_) | PropertyRef::Event(_) => {
            let prop_col = property_col(ctx, md, &property).await?;
            named_property_expression(prop_col, operation, value)
        }
        PropertyRef::Custom(_) => unimplemented!(),
    }
}


pub async fn property_col(
    ctx: &Context,
    md: &Arc<Metadata>,
    property: &PropertyRef,
) -> Result<Expr> {
    Ok(match property {
        PropertyRef::User(prop_name) => {
            let prop = md
                .user_properties
                .get_by_name(ctx.organization_id, ctx.project_id, prop_name)
                .await?;
            col(prop.column_name(Namespace::User).as_str())
        }
        PropertyRef::Event(prop_name) => {
            let prop = md
                .event_properties
                .get_by_name(ctx.organization_id, ctx.project_id, prop_name)
                .await?;
            col(prop.column_name(Namespace::Event).as_str())
        }
        PropertyRef::Custom(_prop_name) => unimplemented!(),
    })
}

/// builds "[property] [op] [values]" binary expression with already known property column
pub fn named_property_expression(
    prop_col: Expr,
    operation: &PropValueOperation,
    values: Option<Vec<ScalarValue>>,
) -> Result<Expr> {
    match operation {
        PropValueOperation::Eq | PropValueOperation::Neq => {
            // expressions for OR
            let mut exprs: Vec<Expr> = vec![];

            let values_vec = values.ok_or_else(|| {
                Error::QueryError("value should be defined for this kind of operation".to_owned())
            })?;

            Ok(match values_vec.len() {
                1 => binary_expr(
                    prop_col.clone(),
                    operation.clone().into(),
                    lit(values_vec[0].clone()),
                ),
                _ => {
                    // iterate over all possible values
                    let exprs = values_vec
                        .iter()
                        .map(|v| {
                            binary_expr(prop_col.clone(), operation.clone().into(), lit(v.clone()))
                        })
                        .collect();

                    multi_or(exprs)
                }
            })
        }
        // for isNull and isNotNull we don't need values at all
        PropValueOperation::Empty => Ok(prop_col.is_null()),
        PropValueOperation::Exists => Ok(prop_col.is_not_null()),
        _ => unimplemented!(),
    }
}

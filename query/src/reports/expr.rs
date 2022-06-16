use std::ops::Sub;
use std::sync::Arc;
use arrow::datatypes::{DataType, Schema};
use chrono::{DateTime, Utc};
use chronoutil::RelativeDuration;
use futures::executor;
use datafusion::logical_plan::ExprSchemable;
use datafusion_common::{Column, ExprSchema, ScalarValue};
use datafusion_expr::{col, Expr, lit, Operator};
use datafusion_expr::expr_fn::{and, binary_expr};
use metadata::{dictionaries, Metadata};
use metadata::properties::provider::Namespace;
use crate::{Context, Error, event_fields};
use crate::logical_plan::expr::{lit_timestamp, multi_or};
use crate::reports::types::{PropertyRef, PropValueOperation, QueryTime};
use crate::Result;

/// builds expression on timestamp
pub fn time_expression<S: ExprSchema>(ts_col_name: &str, schema: &S, time: &QueryTime, cur_time: DateTime<Utc>) -> Result<Expr> {
    let ts_col = Expr::Column(Column::from_qualified_name(ts_col_name));
    let ts_type = ts_col.get_type(schema)?;

    let (from, to) = time.range(cur_time);
    let from_expr = binary_expr(
        ts_col.clone(),
        Operator::GtEq,
        lit_timestamp(ts_type.clone(), &from)?,
    );

    let to_expr = binary_expr(
        ts_col,
        Operator::LtEq,
        lit_timestamp(ts_type, &to)?,
    );

    Ok(and(from_expr, to_expr))
}

pub async fn values_to_dict_keys(
    ctx: &Context,
    dictionaries: &Arc<dictionaries::Provider>,
    dict_type: &DataType,
    col_name: &str,
    values: &Vec<ScalarValue>,
) -> Result<Vec<ScalarValue>> {
    let mut ret: Vec<ScalarValue> = Vec::with_capacity(values.len());
    for value in values.iter() {
        if let ScalarValue::Utf8(inner) = value {
            if let Some(str_value) = inner {
                let key = dictionaries.get_key(
                    ctx.organization_id,
                    ctx.project_id,
                    col_name,
                    str_value.as_str(),
                ).await?;

                let scalar_value = match dict_type {
                    DataType::UInt8 => ScalarValue::UInt8(Some(key as u8)),
                    DataType::UInt16 => ScalarValue::UInt16(Some(key as u16)),
                    DataType::UInt32 => ScalarValue::UInt32(Some(key as u32)),
                    DataType::UInt64 => ScalarValue::UInt64(Some(key as u64)),
                    _ => return Err(Error::QueryError("value type should be string".to_owned()))
                };

                ret.push(scalar_value);
            } else {
                ret.push(ScalarValue::try_from(dict_type)?);
            }
        } else {
            return Err(Error::QueryError("value type should be string".to_owned()));
        }
    }

    return Ok(ret);
}

/// builds name [property] [operation] [value] expression
pub async fn property_expression(
    ctx: &Context,
    md: &Arc<Metadata>,
    property: &PropertyRef,
    operation: &PropValueOperation,
    values: Option<Vec<ScalarValue>>,
) -> Result<Expr> {
    match property {
        PropertyRef::User(prop_name) => {
            let prop = md
                .user_properties
                .get_by_name(ctx.organization_id, ctx.project_id, prop_name)
                .await?;
            let col_name = prop.column_name(Namespace::User);
            let col = col(col_name.as_str());

            if values.is_none() {
                return named_property_expression(col, operation, None);
            }

            if let Some(dict_type) = prop.dictionary_type {
                let dict_values = values_to_dict_keys(
                    &ctx,
                    &md.dictionaries,
                    &dict_type,
                    col_name.as_str(),
                    &values.unwrap(),
                ).await?;
                return named_property_expression(col, operation, Some(dict_values));
            } else {
                return named_property_expression(col, operation, values);
            };
        }
        PropertyRef::Event(prop_name) => {
            let prop = md
                .event_properties
                .get_by_name(ctx.organization_id, ctx.project_id, prop_name)
                .await?;
            let col_name = prop.column_name(Namespace::Event);
            let col = col(col_name.as_str());

            if values.is_none() {
                return named_property_expression(col, operation, values);
            }

            if let Some(dict_type) = prop.dictionary_type {
                let dict_values = values_to_dict_keys(
                    &ctx,
                    &md.dictionaries,
                    &dict_type,
                    col_name.as_str(),
                    &values.unwrap(),
                ).await?;
                return named_property_expression(col, operation, Some(dict_values));
            } else {
                return named_property_expression(col, operation, values);
            };
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
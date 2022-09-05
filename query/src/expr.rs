use std::sync::Arc;

use arrow::datatypes::DataType;
use chrono::{DateTime, Utc};
use datafusion::logical_plan::ExprSchemable;
use datafusion_common::{Column, ExprSchema, ScalarValue};
use datafusion_expr::{col, Expr, lit, Operator};
use datafusion_expr::expr_fn::{and, binary_expr};

use metadata::{dictionaries, Metadata};
use metadata::properties::provider::Namespace;

use crate::{event_fields, Result};
use crate::Context;
use crate::error::QueryError;
use crate::logical_plan::expr::{lit_timestamp, multi_or};
use crate::queries::types::{EventRef, PropertyRef, PropValueOperation, QueryTime};

/// builds expression on timestamp
pub fn time_expression<S: ExprSchema>(
    ts_col_name: &str,
    schema: &S,
    time: &QueryTime,
    cur_time: DateTime<Utc>,
) -> Result<Expr> {
    let ts_col = Expr::Column(Column::from_qualified_name(ts_col_name));
    let ts_type = ts_col.get_type(schema)?;

    let (from, to) = time.range(cur_time);
    let from_expr = binary_expr(
        ts_col.clone(),
        Operator::GtEq,
        lit_timestamp(ts_type.clone(), &from)?,
    );

    let to_expr = binary_expr(ts_col, Operator::LtEq, lit_timestamp(ts_type, &to)?);

    Ok(and(from_expr, to_expr))
}

/// builds expression for event
pub async fn event_expression(
    ctx: &Context,
    metadata: &Arc<Metadata>,
    event: &EventRef,
) -> Result<Expr> {
    Ok(match &event {
        // regular event
        EventRef::Regular(name) => {
            let e = metadata
                .events
                .get_by_name(ctx.organization_id, ctx.project_id, name)
                .await?;

            binary_expr(
                col(event_fields::EVENT),
                Operator::Eq,
                lit(ScalarValue::from(e.id)),
            )
        }

        EventRef::Custom(_event_name) => unimplemented!(),
    })
}

pub async fn encode_property_dict_values(
    ctx: &Context,
    dictionaries: &Arc<dictionaries::Provider>,
    dict_type: &DataType,
    col_name: &str,
    values: &[ScalarValue],
) -> Result<Vec<ScalarValue>> {
    let mut ret: Vec<ScalarValue> = Vec::with_capacity(values.len());
    for value in values.iter() {
        if let ScalarValue::Utf8(inner) = value {
            if let Some(str_value) = inner {
                let key = dictionaries
                    .get_key(
                        ctx.organization_id,
                        ctx.project_id,
                        col_name,
                        str_value.as_str(),
                    )
                    .await?;

                let scalar_value = match dict_type {
                    DataType::UInt8 => ScalarValue::UInt8(Some(key as u8)),
                    DataType::UInt16 => ScalarValue::UInt16(Some(key as u16)),
                    DataType::UInt32 => ScalarValue::UInt32(Some(key as u32)),
                    DataType::UInt64 => ScalarValue::UInt64(Some(key as u64)),
                    _ => return Err(QueryError::Plan(format!("unsupported dictionary type \"{:?}\"", dict_type)))
                };

                ret.push(scalar_value);
            } else {
                ret.push(ScalarValue::try_from(dict_type)?);
            }
        } else {
            return Err(QueryError::Plan(format!("value type should be Utf8, but \"{:?}\" was given", value.get_datatype())));
        }
    }

    Ok(ret)
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
                let dict_values = encode_property_dict_values(
                    ctx,
                    &md.dictionaries,
                    &dict_type,
                    col_name.as_str(),
                    &values.unwrap(),
                )
                    .await?;
                named_property_expression(col, operation, Some(dict_values))
            } else {
                named_property_expression(col, operation, values)
            }
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
                let dict_values = encode_property_dict_values(
                    ctx,
                    &md.dictionaries,
                    &dict_type,
                    col_name.as_str(),
                    &values.unwrap(),
                )
                    .await?;
                named_property_expression(col, operation, Some(dict_values))
            } else {
                named_property_expression(col, operation, values)
            }
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
        PropValueOperation::Eq | PropValueOperation::Neq | PropValueOperation::Like => {
            // expressions for OR
            let values_vec = values.ok_or_else(|| {
                QueryError::Plan(format!("value should be defined for \"{:?}\" operation", operation))
            })?;

            Ok(match values_vec.len() {
                1 => binary_expr(
                    prop_col,
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

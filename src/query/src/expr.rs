use std::sync::Arc;

use arrow::datatypes::DataType;
use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono::Utc;
use common::query::event_segmentation::Breakdown;
use common::query::EventFilter;
use common::query::EventRef;
use common::query::PropValueOperation;
use common::query::PropertyRef;
use common::query::QueryTime;
use common::types::COLUMN_EVENT;
use datafusion_common::Column;
use datafusion_common::ExprSchema;
use datafusion_common::ScalarValue;
use datafusion_expr::col;
use datafusion_expr::expr_fn::and;
use datafusion_expr::expr_fn::binary_expr;
use datafusion_expr::lit;
use datafusion_expr::or;
use datafusion_expr::Expr;
use datafusion_expr::ExprSchemable;
use datafusion_expr::Operator;
use metadata::dictionaries::Dictionaries;
use metadata::properties::DictionaryType;
use metadata::MetadataProvider;

use crate::error::QueryError;
use crate::logical_plan::expr::lit_timestamp;
use crate::logical_plan::expr::multi_and;
use crate::logical_plan::expr::multi_or;
use crate::Context;
use crate::Result;

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
pub fn event_expression(
    ctx: &Context,
    metadata: &Arc<MetadataProvider>,
    event: &EventRef,
) -> Result<Expr> {
    Ok(match &event {
        // regular event
        EventRef::RegularName(name) => {
            let e = metadata.events.get_by_name(ctx.project_id, name)?;
            binary_expr(
                col(COLUMN_EVENT),
                Operator::Eq,
                lit(ScalarValue::from(e.id as u16)),
            )
        }
        EventRef::Regular(id) => binary_expr(
            col(COLUMN_EVENT),
            Operator::Eq,
            lit(ScalarValue::from(*id as u16)),
        ),

        EventRef::Custom(id) => {
            let e = metadata.custom_events.get_by_id(ctx.project_id, *id)?;
            let mut exprs: Vec<Expr> = Vec::new();
            for event in e.events.iter() {
                let mut expr = match &event.event {
                    EventRef::RegularName(name) => {
                        event_expression(ctx, metadata, &EventRef::RegularName(name.to_owned()))?
                    }
                    EventRef::Regular(id) => {
                        event_expression(ctx, metadata, &EventRef::Regular(*id))?
                    }
                    EventRef::Custom(id) => {
                        event_expression(ctx, metadata, &EventRef::Custom(*id))?
                    }
                };

                if let Some(filters) = &event.filters {
                    expr = and(
                        expr.clone(),
                        event_filters_expression(ctx, metadata, filters)?,
                    );
                }

                exprs.push(expr);
            }

            let mut final_expr = exprs[0].clone();
            for expr in exprs.iter().skip(1) {
                final_expr = or(final_expr.clone(), expr.to_owned())
            }

            final_expr
        }
    })
}

/// builds event filters expression
pub fn event_filters_expression(
    ctx: &Context,
    metadata: &Arc<MetadataProvider>,
    filters: &[EventFilter],
) -> Result<Expr> {
    // iterate over filters
    let filters_exprs = filters
        .iter()
        .map(|filter| {
            // match filter type
            match filter {
                EventFilter::Property {
                    property,
                    operation,
                    value,
                } => property_expression(ctx, metadata, property, operation, value.clone()),
            }
        })
        .collect::<Result<Vec<Expr>>>()?;

    if filters_exprs.len() == 1 {
        return Ok(filters_exprs[0].clone());
    }

    Ok(multi_and(filters_exprs))
}

// builds breakdown expression
pub fn breakdown_expr(
    ctx: &Context,
    metadata: &Arc<MetadataProvider>,
    breakdown: &Breakdown,
) -> crate::Result<Expr> {
    match breakdown {
        Breakdown::Property(prop_ref) => match prop_ref {
            PropertyRef::System(_prop_name)
            | PropertyRef::User(_prop_name)
            | PropertyRef::Event(_prop_name) => {
                let prop_col = property_col(ctx, metadata, prop_ref)?;
                Ok(prop_col)
            }
            PropertyRef::Custom(_) => unimplemented!(),
        },
    }
}

pub fn encode_property_dict_values(
    ctx: &Context,
    dictionaries: &Arc<Dictionaries>,
    dict_type: &DictionaryType,
    col_name: &str,
    values: &[ScalarValue],
) -> Result<Vec<ScalarValue>> {
    let mut ret: Vec<ScalarValue> = Vec::with_capacity(values.len());
    for value in values.iter() {
        if let ScalarValue::Utf8(inner) = value {
            if let Some(str_value) = inner {
                let key = dictionaries.get_key(ctx.project_id, col_name, str_value.as_str())?;

                let scalar_value = match dict_type {
                    DictionaryType::Int8 => ScalarValue::Int8(Some(key as i8)),
                    DictionaryType::Int16 => ScalarValue::Int16(Some(key as i16)),
                    DictionaryType::Int32 => ScalarValue::Int32(Some(key as i32)),
                    DictionaryType::Int64 => ScalarValue::Int64(Some(key as i64)),
                };

                ret.push(scalar_value);
            } else {
                ret.push(ScalarValue::try_from(DataType::from(dict_type.to_owned()))?);
            }
        } else {
            #[allow(deprecated)]
            return Err(QueryError::Plan(format!(
                "value type should be Utf8, but \"{:?}\" was given",
                value.get_datatype()
            )));
        }
    }

    Ok(ret)
}

fn prop_expression(
    ctx: &Context,
    prop: &Arc<metadata::properties::Properties>,
    dicts: &Arc<metadata::dictionaries::Dictionaries>,
    prop_name: &str,
    operation: &PropValueOperation,
    values: Option<Vec<ScalarValue>>,
) -> Result<Expr> {
    let prop = prop.get_by_name(ctx.project_id, prop_name)?;
    let col_name = prop.column_name();
    let col = col(col_name.as_str());

    if values.is_none() {
        return named_property_expression(col, operation, None);
    }

    match operation {
        PropValueOperation::Like
        | PropValueOperation::NotLike
        | PropValueOperation::Regex
        | PropValueOperation::NotRegex => {
            return named_property_expression(col, operation, values);
        }
        _ => {}
    }

    if let Some(dict_type) = prop.dictionary_type {
        let dict_values = encode_property_dict_values(
            ctx,
            dicts,
            &dict_type,
            col_name.as_str(),
            &values.unwrap(),
        )?;
        named_property_expression(col, operation, Some(dict_values))
    } else {
        named_property_expression(col, operation, values)
    }
}

/// builds name [property] [operation] [value] expression
pub fn property_expression(
    ctx: &Context,
    md: &Arc<MetadataProvider>,
    property: &PropertyRef,
    operation: &PropValueOperation,
    values: Option<Vec<ScalarValue>>,
) -> Result<Expr> {
    match property {
        PropertyRef::System(prop_name) => prop_expression(
            ctx,
            &md.system_properties,
            &md.dictionaries,
            prop_name,
            operation,
            values,
        ),
        PropertyRef::User(prop_name) => prop_expression(
            ctx,
            &md.user_properties,
            &md.dictionaries,
            prop_name,
            operation,
            values,
        ),
        PropertyRef::Event(prop_name) => prop_expression(
            ctx,
            &md.event_properties,
            &md.dictionaries,
            prop_name,
            operation,
            values,
        ),
        PropertyRef::Custom(_) => unimplemented!(),
    }
}

pub fn property_col(
    ctx: &Context,
    md: &Arc<MetadataProvider>,
    property: &PropertyRef,
) -> Result<Expr> {
    Ok(match property {
        PropertyRef::System(prop_name) => {
            let prop = md
                .system_properties
                .get_by_name(ctx.project_id, prop_name)?;
            col(prop.column_name().as_str())
        }
        PropertyRef::User(prop_name) => {
            let prop = md.user_properties.get_by_name(ctx.project_id, prop_name)?;
            col(prop.column_name().as_str())
        }
        PropertyRef::Event(prop_name) => {
            let prop = md.event_properties.get_by_name(ctx.project_id, prop_name)?;
            col(prop.column_name().as_str())
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
        PropValueOperation::Eq
        | PropValueOperation::Neq
        | PropValueOperation::Gt
        | PropValueOperation::Gte
        | PropValueOperation::Lt
        | PropValueOperation::Lte
        | PropValueOperation::Regex
        | PropValueOperation::NotRegex => {
            // expressions for OR
            let values_vec = values.ok_or_else(|| {
                QueryError::Plan(format!(
                    "value should be defined for \"{operation:?}\" operation"
                ))
            })?;

            Ok(match values_vec.len() {
                1 => binary_expr(
                    prop_col,
                    operation.to_owned().try_into()?,
                    lit(values_vec[0].to_owned()),
                ),
                _ => {
                    // iterate over all possible values
                    let exprs = values_vec
                        .iter()
                        .map(|v| {
                            binary_expr(
                                prop_col.clone(),
                                operation.clone().try_into().unwrap(),
                                lit(v.to_owned()),
                            )
                        })
                        .collect();

                    multi_or(exprs)
                }
            })
        }
        PropValueOperation::Like => {
            // expressions for OR
            let values_vec = values.ok_or_else(|| {
                QueryError::Plan(format!(
                    "value should be defined for \"{operation:?}\" operation"
                ))
            })?;

            Ok(match values_vec.len() {
                1 => prop_col.like(lit(values_vec[0].to_owned())),
                _ => {
                    // iterate over all possible values
                    let exprs = values_vec
                        .iter()
                        .map(|v| prop_col.to_owned().like(lit(v.to_owned())))
                        .collect();

                    multi_or(exprs)
                }
            })
        }
        PropValueOperation::NotLike => {
            // expressions for OR
            let values_vec = values.ok_or_else(|| {
                QueryError::Plan(format!(
                    "value should be defined for \"{operation:?}\" operation"
                ))
            })?;

            Ok(match values_vec.len() {
                1 => prop_col.not_like(lit(values_vec[0].to_owned())),
                _ => {
                    // iterate over all possible values
                    let exprs = values_vec
                        .iter()
                        .map(|v| prop_col.to_owned().not_like(lit(v.to_owned())))
                        .collect();

                    multi_or(exprs)
                }
            })
        }
        // for isNull and isNotNull we don't need values at all
        PropValueOperation::True => Ok(prop_col.is_true()),
        PropValueOperation::False => Ok(prop_col.is_false()),
        PropValueOperation::Empty => Ok(prop_col.is_null()),
        PropValueOperation::Exists => Ok(prop_col.is_not_null()),
    }
}

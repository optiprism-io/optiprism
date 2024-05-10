use std::collections::HashMap;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common::group_col;
// use std::time::Duration;
use common::query::funnel::ChartType;
use common::query::funnel::Count;
use common::query::funnel::ExcludeSteps;
use common::query::funnel::Filter;
use common::query::funnel::Funnel;
use common::query::funnel::Order;
use common::query::funnel::StepOrder;
use common::query::funnel::TimeIntervalUnitSession;
use common::query::funnel::TimeWindow;
use common::query::funnel::Touch;
use common::query::Breakdown;
use common::query::EventRef;
use common::query::PropValueFilter;
use common::query::PropertyRef;
use common::query::QueryTime;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_PROJECT_ID;
use datafusion_common::Column;
use datafusion_common::ScalarValue;
use datafusion_expr::and;
use datafusion_expr::binary_expr;
use datafusion_expr::col;
use datafusion_expr::expr;
use datafusion_expr::lit;
use datafusion_expr::Expr;
use datafusion_expr::Extension;
use datafusion_expr::Filter as PlanFilter;
use datafusion_expr::Limit;
use datafusion_expr::LogicalPlan;
use datafusion_expr::Operator;
use datafusion_expr::Projection;
use datafusion_expr::Sort;
use metadata::dictionaries::SingleDictionaryProvider;
use metadata::MetadataProvider;
use rust_decimal::Decimal;

use crate::breakdowns_to_dicts;
use crate::error::QueryError;
use crate::error::Result;
use crate::expr::event_expression;
use crate::expr::event_filters_expression;
use crate::expr::property_col;
use crate::expr::time_expression;
use crate::logical_plan;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::expr::multi_or;
use crate::logical_plan::funnel::FunnelNode;
use crate::logical_plan::rename_columns::RenameColumnsNode;
use crate::logical_plan::SortField;
use crate::queries::decode_filter_single_dictionary;
use crate::queries::event_records_search::Event;
use crate::Context;

pub fn build(
    ctx: Context,
    metadata: Arc<MetadataProvider>,
    mut input: LogicalPlan,
    req: Funnel,
) -> Result<LogicalPlan> {
    let mut cols_hash: HashMap<String, ()> = HashMap::new();
    let input = decode_filter_dictionaries(&ctx, &metadata, &req, input, &mut cols_hash)?;

    let mut expr = col(Column {
        relation: None,
        name: COLUMN_PROJECT_ID.to_string(),
    });

    expr = time_expression(COLUMN_CREATED_AT, input.schema(), &req.time, ctx.cur_time)?;
    if let Some(filters) = &req.filters {
        expr = and(expr, event_filters_expression(&ctx, &metadata, filters)?)
    }
    let input = LogicalPlan::Filter(PlanFilter::try_new(expr, Arc::new(input))?);

    let (from, to) = req.time.range(ctx.cur_time);

    let window = match req.time_window {
        TimeWindow { n, unit } => unit.duration(n as i64),
    };

    let steps = {
        let mut steps = vec![];
        for step in req.steps.iter() {
            let mut exprs = vec![];
            for event in step.events.iter() {
                let mut expr = event_expression(&ctx, &metadata, &event.event, req.group_id)?;
                if let Some(filters) = &event.filters {
                    expr = and(expr, event_filters_expression(&ctx, &metadata, &filters)?);
                }
                exprs.push(expr)
            }

            let order = match &step.order {
                StepOrder::Exact => logical_plan::funnel::StepOrder::Exact,
                StepOrder::Any(order) => logical_plan::funnel::StepOrder::Any(order.to_vec()),
            };
            steps.push((multi_or(exprs), order))
        }

        steps
    };

    let exclude = if let Some(excludes) = &req.exclude {
        let mut out = vec![];
        for exclude in excludes {
            let mut expr = event_expression(&ctx, &metadata, &exclude.event.event, req.group_id)?;
            if let Some(filters) = &exclude.event.filters {
                expr = and(expr, event_filters_expression(&ctx, &metadata, &filters)?);
            }
            let steps = if let Some(steps) = &exclude.steps {
                let steps = match steps {
                    ExcludeSteps::All => logical_plan::funnel::ExcludeSteps {
                        from: 0,
                        to: req.steps.len() - 1,
                    },
                    ExcludeSteps::Between(from, to) => logical_plan::funnel::ExcludeSteps {
                        from: *from,
                        to: *to,
                    },
                };
                Some(steps)
            } else {
                None
            };

            out.push(logical_plan::funnel::ExcludeExpr { expr, steps });
        }

        Some(out)
    } else {
        None
    };

    let constants = if let Some(constants) = &req.holding_constants {
        let mut out = vec![];
        for constant in constants {
            let expr = property_col(&ctx, &metadata, constant)?;
            out.push(expr);
        }

        Some(out)
    } else {
        None
    };

    let mut rename_groups = vec![];
    let mut decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)> = Vec::new();
    let groups = if let Some(breakdowns) = &req.breakdowns {
        breakdowns_to_dicts!(metadata, ctx, breakdowns, cols_hash, decode_cols);
        let mut out = vec![];
        for breakdown in breakdowns {
            let prop = match breakdown {
                Breakdown::Property(p) => match p {
                    PropertyRef::System(p) => {
                        metadata.system_properties.get_by_name(ctx.project_id, p)?
                    }
                    PropertyRef::Group(p, group) => {
                        metadata.group_properties[*group].get_by_name(ctx.project_id, p)?
                    }
                    PropertyRef::Event(p) => {
                        metadata.event_properties.get_by_name(ctx.project_id, p)?
                    }
                    _ => {
                        return Err(QueryError::Unimplemented(
                            "invalid property type".to_string(),
                        ));
                    }
                },
            };

            rename_groups.push((prop.column_name(), prop.name()));

            let expr = col(Column {
                relation: None,
                name: prop.column_name(),
            });
            out.push((expr, prop.column_name(), SortField {
                data_type: prop.data_type(),
            }))
        }
        Some(out)
    } else {
        None
    };
    let funnel = logical_plan::funnel::Funnel {
        ts_col: Column {
            relation: None,
            name: COLUMN_CREATED_AT.to_string(),
        },
        from,
        to,
        window,
        steps,
        exclude,
        constants,
        count: match req.count {
            Count::Unique => logical_plan::funnel::Count::Unique,
            Count::NonUnique => logical_plan::funnel::Count::NonUnique,
            Count::Session => logical_plan::funnel::Count::Session,
        },
        filter: req.filter.map(|f| match f {
            Filter::DropOffOnAnyStep => logical_plan::funnel::Filter::DropOffOnAnyStep,
            Filter::DropOffOnStep(n) => logical_plan::funnel::Filter::DropOffOnStep(n),
            Filter::TimeToConvert(from, to) => logical_plan::funnel::Filter::TimeToConvert(
                chrono::Duration::milliseconds(from),
                chrono::Duration::milliseconds(to),
            ),
        }),
        touch: match req.touch {
            Touch::First => logical_plan::funnel::Touch::First,
            Touch::Last => logical_plan::funnel::Touch::Last,
            Touch::Step { step } => logical_plan::funnel::Touch::Step(step),
        },
        partition_col: col(Column {
            relation: None,
            name: group_col(req.group_id),
        }),
        time_interval: req.chart_type.time_interval(),
        groups,
    };
    let input = LogicalPlan::Extension(Extension {
        node: Arc::new(FunnelNode::new(
            input,
            None,
            Column::from_qualified_name(group_col(req.group_id)),
            funnel,
        )?),
    });

    let input = if !decode_cols.is_empty() {
        LogicalPlan::Extension(Extension {
            node: Arc::new(DictionaryDecodeNode::try_new(input, decode_cols.clone())?),
        })
    } else {
        input
    };

    let input = LogicalPlan::Extension(Extension {
        node: Arc::new(RenameColumnsNode::try_new(input, rename_groups)?),
    });

    Ok(input)
}

fn decode_filter_dictionaries(
    ctx: &Context,
    metadata: &Arc<MetadataProvider>,
    funnel: &Funnel,
    input: LogicalPlan,
    cols_hash: &mut HashMap<String, ()>,
) -> Result<LogicalPlan> {
    let mut decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)> = Vec::new();

    for step in &funnel.steps {
        for event in &step.events {
            if let Some(filters) = &event.filters {
                for filter in filters {
                    decode_filter_single_dictionary(
                        ctx,
                        metadata,
                        cols_hash,
                        &mut decode_cols,
                        filter,
                    )?;
                }
            }
        }
    }

    if let Some(filters) = &funnel.filters {
        for filter in filters {
            decode_filter_single_dictionary(ctx, metadata, cols_hash, &mut decode_cols, filter)?;
        }
    }
    if decode_cols.is_empty() {
        return Ok(input);
    }
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(DictionaryDecodeNode::try_new(input, decode_cols.clone())?),
    }))
}
#[derive(Clone, Debug)]
pub struct StepData {
    pub groups: Option<Vec<String>>,
    pub ts: i64,
    pub total: i64,
    pub conversion_ratio: Decimal,
    pub avg_time_to_convert: Decimal,
    pub dropped_off: i64,
    pub drop_off_ratio: Decimal,
    pub time_to_convert: i64,
    pub time_to_convert_from_start: i64,
}
#[derive(Clone, Debug)]
pub struct Step {
    pub step: String,
    pub data: Vec<StepData>,
}
#[derive(Clone, Debug)]
pub struct Response {
    pub steps: Vec<Step>,
}

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::Array;
use arrow::array::Decimal128Array;
use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::array::StringBuilder;
use arrow::array::TimestampMillisecondArray;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use common::funnel::Count;
use common::funnel::ExcludeSteps;
use common::funnel::Filter;
use common::funnel::Funnel;
use common::funnel::StepOrder;
use common::funnel::TimeWindow;
use common::funnel::Touch;
use common::group_col;
// use std::time::Duration;
use common::query::Breakdown;
use common::query::EventRef;
use common::query::PropValueFilter;
use common::query::PropertyRef;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_PROJECT_ID;
use common::types::METRIC_QUERY_EXECUTION_TIME_SECONDS;
use common::types::METRIC_QUERY_QUERIES_TOTAL;
use common::types::ROUND_DIGITS;
use common::types::TABLE_EVENTS;
use common::DECIMAL_SCALE;
use datafusion_common::Column;
use datafusion_expr::and;
use datafusion_expr::col;
use datafusion_expr::Extension;
use datafusion_expr::Filter as PlanFilter;
use datafusion_expr::LogicalPlan;
use metadata::dictionaries::SingleDictionaryProvider;
use metadata::MetadataProvider;
use metrics::counter;
use metrics::histogram;
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use storage::db::OptiDBImpl;
use tracing::debug;

use crate::breakdowns_to_dicts;
use crate::col_name;
use crate::decode_filter_single_dictionary;
use crate::error::QueryError;
use crate::error::Result;
use crate::execute;
use crate::expr::event_expression;
use crate::expr::event_filters_expression;
use crate::expr::property_col;
use crate::expr::time_expression;
use crate::fix_filter;
use crate::initial_plan;
use crate::logical_plan;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::expr::multi_or;
use crate::logical_plan::funnel::FunnelNode;
use crate::logical_plan::SortField;
use crate::Context;

pub struct FunnelProvider {
    metadata: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
}

impl FunnelProvider {
    pub fn new(metadata: Arc<MetadataProvider>, db: Arc<OptiDBImpl>) -> Self {
        Self { metadata, db }
    }

    pub async fn funnel(&self, ctx: Context, req: Funnel) -> Result<Response> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = projection(&ctx, &req, &self.metadata)?;
        let projection = projection
            .iter()
            .map(|x| schema.index_of(x).unwrap())
            .collect();

        let (session_ctx, state, plan) =
            initial_plan(&self.db, TABLE_EVENTS.to_string(), projection)?;
        let plan = build(ctx.clone(), self.metadata.clone(), plan, req.clone())?;

        println!("{plan:?}");
        let result = execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        let elapsed = start.elapsed();
        histogram!(METRIC_QUERY_EXECUTION_TIME_SECONDS, "query"=>"funnel").record(elapsed);
        counter!(METRIC_QUERY_QUERIES_TOTAL,"query"=>"funnel").increment(1);
        debug!("elapsed: {:?}", duration);
        let mut group_cols: Vec<StringArray> = vec![];
        let ts_col = {
            let idx = result.schema().index_of("ts").unwrap();
            result
                .column(idx)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
        };

        // segment
        let col = cast(&result.column(0), &DataType::Utf8)?
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .to_owned();
        group_cols.push(col);
        let mut groups = vec!["Segment".to_string()];
        if let Some(breakdowns) = &req.breakdowns {
            for (idx, breakdown) in breakdowns.iter().enumerate() {
                let prop = match breakdown {
                    Breakdown::Property(prop) => match prop {
                        PropertyRef::Group(name, gid) => self.metadata.group_properties[*gid]
                            .get_by_name(ctx.project_id, name.as_ref())?,
                        PropertyRef::Event(name) => self
                            .metadata
                            .event_properties
                            .get_by_name(ctx.project_id, name.as_ref())?,
                        PropertyRef::Custom(_) => {
                            return Err(QueryError::Unimplemented(
                                "custom properties are not implemented for breakdowns".to_string(),
                            ));
                        }
                    },
                };
                let col = match result.column(idx + 1).data_type() {
                    DataType::Decimal128(_, _) => {
                        let arr = result
                            .column(idx + 1)
                            .as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap();
                        let mut builder = StringBuilder::new();
                        for i in 0..arr.len() {
                            let v = arr.value(i);
                            let vv = Decimal::from_i128_with_scale(v, DECIMAL_SCALE as u32)
                                .round_dp(ROUND_DIGITS.into());
                            if vv.is_integer() {
                                builder.append_value(vv.to_i64().unwrap().to_string());
                            } else {
                                builder.append_value(vv.to_string());
                            }
                        }
                        builder
                            .finish()
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .clone()
                    }
                    _ => cast(result.column(idx + 1), &DataType::Utf8)?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .to_owned(),
                };

                group_cols.push(col);
                groups.push(prop.name());
            }
        }

        let mut int_val_cols = vec![];
        let mut dec_val_cols = vec![];

        for idx in 0..(result.schema().fields().len() - group_cols.len() - 1) {
            let arr = result.column(group_cols.len() + 1 + idx).to_owned();
            match arr.data_type() {
                DataType::Int64 => int_val_cols.push(
                    arr.as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .to_owned(),
                ),
                DataType::Decimal128(_, _) => dec_val_cols.push(
                    arr.as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .to_owned(),
                ),
                _ => panic!("unexpected data type"),
            }
        }

        let mut steps = vec![];
        for (step_id, step) in req.steps.iter().enumerate() {
            let name = match &step.events[0].event {
                EventRef::RegularName(n) => n.clone(),
                EventRef::Regular(id) => self.metadata.events.get_by_id(ctx.project_id, *id)?.name,
                EventRef::Custom(_) => unimplemented!(),
            };
            let mut step = Step {
                step: name.clone(),
                data: vec![],
            };
            for idx in 0..result.num_rows() {
                let groups = if group_cols.is_empty() {
                    None
                } else {
                    Some(
                        group_cols
                            .iter()
                            .map(|col| col.value(idx).to_string())
                            .collect::<Vec<_>>(),
                    )
                };
                let step_data = StepData {
                    groups,
                    ts: ts_col.value(idx),
                    total: int_val_cols[step_id * 4].value(idx),
                    conversion_ratio: Decimal::from_i128_with_scale(
                        dec_val_cols[step_id * 4].value(idx),
                        DECIMAL_SCALE as u32,
                    )
                    .round_dp(ROUND_DIGITS.into()),
                    avg_time_to_convert: Decimal::from_i128_with_scale(
                        dec_val_cols[step_id * 4 + 1].value(idx),
                        DECIMAL_SCALE as u32,
                    )
                    .round_dp(ROUND_DIGITS.into()),
                    avg_time_to_convert_from_start: Decimal::from_i128_with_scale(
                        dec_val_cols[step_id * 4 + 2].value(idx),
                        DECIMAL_SCALE as u32,
                    )
                    .round_dp(ROUND_DIGITS.into()),
                    dropped_off: int_val_cols[step_id * 4 + 1].value(idx),
                    drop_off_ratio: Decimal::from_i128_with_scale(
                        dec_val_cols[step_id * 4 + 3].value(idx),
                        DECIMAL_SCALE as u32,
                    )
                    .round_dp(ROUND_DIGITS.into()),
                    time_to_convert: int_val_cols[step_id * 4 + 2].value(idx),
                    time_to_convert_from_start: int_val_cols[step_id * 4 + 3].value(idx),
                };
                step.data.push(step_data);
            }
            steps.push(step);
        }

        Ok(Response { groups, steps })
    }
}

pub fn build(
    ctx: Context,
    metadata: Arc<MetadataProvider>,
    input: LogicalPlan,
    req: Funnel,
) -> Result<LogicalPlan> {
    let mut cols_hash: HashMap<String, ()> = HashMap::new();
    let input = decode_filter_dictionaries(&ctx, &metadata, &req, input, &mut cols_hash)?;

    let mut expr = time_expression(COLUMN_CREATED_AT, input.schema(), &req.time, ctx.cur_time)?;
    if let Some(filters) = &req.filters {
        expr = and(expr, event_filters_expression(&ctx, &metadata, filters)?)
    }
    let input = LogicalPlan::Filter(PlanFilter::try_new(expr, Arc::new(input))?);

    let (from, to) = req.time.range(ctx.cur_time);

    let TimeWindow { n, unit } = req.time_window;
    let window = unit.duration(n as i64);

    let steps = {
        let mut steps = vec![];
        for step in req.steps.iter() {
            let mut exprs = vec![];
            for event in step.events.iter() {
                let mut expr = event_expression(&ctx, &metadata, &event.event)?;
                if let Some(filters) = &event.filters {
                    expr = and(expr, event_filters_expression(&ctx, &metadata, filters)?);
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
            let mut expr = event_expression(&ctx, &metadata, &exclude.event)?;
            if let Some(filters) = &exclude.filters {
                expr = and(expr, event_filters_expression(&ctx, &metadata, filters)?);
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
        touch: req.touch.map(|t| match t {
            Touch::First => logical_plan::funnel::Touch::First,
            Touch::Last => logical_plan::funnel::Touch::Last,
            Touch::Step { step } => logical_plan::funnel::Touch::Step(step),
        }),
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
    pub avg_time_to_convert_from_start: Decimal,
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
    pub groups: Vec<String>,
    pub steps: Vec<Step>,
}

fn projection(ctx: &Context, req: &Funnel, md: &Arc<MetadataProvider>) -> Result<Vec<String>> {
    let mut fields = vec![
        COLUMN_PROJECT_ID.to_string(),
        group_col(req.group_id),
        COLUMN_CREATED_AT.to_string(),
        COLUMN_EVENT.to_string(),
    ];

    for step in &req.steps {
        for event in &step.events {
            if let Some(filters) = &event.filters {
                for filter in filters {
                    match filter {
                        PropValueFilter::Property { property, .. } => {
                            fields.push(col_name(ctx, property, md)?)
                        }
                    }
                }
            }
        }
    }

    if let Some(c) = &req.holding_constants {
        for constant in c {
            fields.push(col_name(ctx, constant, md)?);
        }
    }

    if let Some(exclude) = &req.exclude {
        for e in exclude {
            if let Some(filters) = &e.filters {
                for filter in filters {
                    match filter {
                        PropValueFilter::Property { property, .. } => {
                            fields.push(col_name(ctx, property, md)?)
                        }
                    }
                }
            }
        }
    }

    if let Some(filters) = &req.filters {
        for filter in filters {
            match filter {
                PropValueFilter::Property { property, .. } => {
                    fields.push(col_name(ctx, property, md)?)
                }
            }
        }
    }

    if let Some(breakdowns) = &req.breakdowns {
        for breakdown in breakdowns {
            match breakdown {
                Breakdown::Property(property) => fields.push(col_name(ctx, property, md)?),
            }
        }
    }

    fields.dedup();
    Ok(fields)
}

pub fn fix_request(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: common::funnel::Funnel,
) -> crate::Result<common::funnel::Funnel> {
    let mut out = req.clone();

    for (step_id, step) in req.steps.iter().enumerate() {
        for (event_id, event) in step.events.iter().enumerate() {
            if let Some(filters) = &event.filters {
                if filters.is_empty() {
                    out.steps[step_id].events[event_id].filters = None;
                }
            }
        }
    }

    if let Some(exclude) = &req.exclude {
        if exclude.is_empty() {
            out.exclude = None;
        } else {
            let mut out_exclude = exclude.to_vec();
            for (exclude_id, exclude) in exclude.iter().enumerate() {
                if let Some(filters) = &exclude.filters {
                    if filters.is_empty() {
                        out_exclude[exclude_id].filters = None;
                    }
                }
            }

            out.exclude = Some(out_exclude)
        }
    }

    if let Some(breakdowns) = &req.breakdowns
        && breakdowns.is_empty()
    {
        out.breakdowns = None;
    }

    if let Some(holding_constants) = &req.holding_constants
        && holding_constants.is_empty()
    {
        out.holding_constants = None;
    }

    if let Some(filters) = &req.filters {
        let mut of = vec![];
        if filters.is_empty() {
            out.filters = None;
        } else {
            for filter in filters.iter() {
                let f = fix_filter(md, project_id, filter)?;
                of.push(f);
            }
            out.filters = Some(of);
        }
    }
    Ok(out)
}

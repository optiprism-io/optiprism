use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::Array;
use arrow::array::ArrayAccessor;
use arrow::array::ArrayRef;
use arrow::array::Decimal128Array;
use arrow::array::Int64Array;
use arrow::array::StringArray;
use arrow::array::StringBuilder;
use arrow::array::TimestampMillisecondArray;
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;
use common::group_col;
use common::event_segmentation::EventSegmentation;
use common::event_segmentation::Query;
use common::funnel::Funnel;
use common::query::Breakdown;
use common::query::EventRef;
use common::query::PropValueFilter;
use common::query::PropertyRef;
use common::types::{COLUMN_CREATED_AT, ROUND_DIGITS};
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use common::types::GROUP_COLUMN_CREATED_AT;
use common::types::GROUP_COLUMN_ID;
use common::types::GROUP_COLUMN_PROJECT_ID;
use common::types::TABLE_EVENTS;
use common::DECIMAL_SCALE;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::parquet::basic::ConvertedType::NONE;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::Extension;
use datafusion_expr::LogicalPlan;
use num_traits::ToPrimitive;
use metadata::MetadataProvider;
use rust_decimal::Decimal;
use storage::db::OptiDBImpl;
use tracing::debug;

use crate::{col_name, execute, initial_plan};
use crate::logical_plan::db_parquet::DbParquetNode;
use crate::physical_plan::planner::QueryPlanner;
use crate::queries::event_segmentation;
use crate::queries::event_segmentation::logical_plan_builder::COL_AGG_NAME;
use crate::queries::funnel;
use crate::queries::funnel::StepData;
use crate::queries::group_records_search;
use crate::queries::group_records_search::GroupRecordsSearch;
use crate::queries::property_values;
use crate::queries::property_values::PropertyValues;
use crate::Column;
use crate::ColumnType;
use crate::Context;
use crate::DataTable;
use crate::error::QueryError;
use crate::Result;

pub struct QueryProvider {
    metadata: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
}

impl QueryProvider {
    pub fn new(metadata: Arc<MetadataProvider>, db: Arc<OptiDBImpl>) -> Self {
        Self { metadata, db }
    }
}

impl QueryProvider {
    pub async fn property_values(&self, ctx: Context, req: PropertyValues) -> Result<ArrayRef> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = property_values_projection(&ctx, &req, &self.metadata)?;
        let projection = projection
            .iter()
            .map(|x| schema.index_of(x).unwrap())
            .collect();
        let (session_ctx, state, plan) = initial_plan(&self.db, projection).await?;
        let plan = property_values::LogicalPlanBuilder::build(
            ctx,
            self.metadata.clone(),
            plan,
            req.clone(),
        )?;

        let res = execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);

        Ok(res.column(0).to_owned())
    }

    pub async fn group_records_search(
        &self,
        ctx: Context,
        req: GroupRecordsSearch,
    ) -> Result<DataTable> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = if req.properties.is_some() {
            let projection = group_records_search_projection(&ctx, &req, &self.metadata)?;
            projection
                .iter()
                .map(|x| schema.index_of(x).unwrap())
                .collect()
        } else {
            (0..schema.fields.len()).collect::<Vec<_>>()
        };

        let (session_ctx, state, plan) = initial_plan(&self.db, projection).await?;
        let plan = group_records_search::build(ctx, self.metadata.clone(), plan, req.clone())?;

        let result = execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);

        let cols = result
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| Column {
                name: field.name().to_owned(),
                typ: ColumnType::Dimension,
                is_nullable: field.is_nullable(),
                data_type: field.data_type().to_owned(),
                hidden: false,
                data: result.column(idx).to_owned(),
            })
            .collect();

        Ok(DataTable::new(result.schema(), cols))
    }

    pub async fn event_segmentation(
        &self,
        ctx: Context,
        req: EventSegmentation,
    ) -> Result<DataTable> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = event_segmentation_projection(&ctx, &req, &self.metadata)?;
        let projection = projection
            .iter()
            .map(|x| schema.index_of(x).unwrap())
            .collect();
        let (session_ctx, state, plan) = initial_plan(&self.db, projection).await?;
        let plan = event_segmentation::logical_plan_builder::LogicalPlanBuilder::build(
            ctx.clone(),
            self.metadata.clone(),
            plan,
            req.clone(),
        )?;

        println!("{plan:?}");
        let result = execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);

        let metric_cols = req.time_columns(ctx.cur_time);
        let cols = result
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let typ = match metric_cols.contains(field.name()) {
                    true => ColumnType::Metric,
                    false => ColumnType::Dimension,
                };

                let arr = result.column(idx).to_owned();

                Column {
                    name: field.name().to_owned(),
                    typ,
                    is_nullable: field.is_nullable(),
                    data_type: field.data_type().to_owned(),
                    hidden: false,
                    data: arr,
                }
            })
            .collect();

        Ok(DataTable::new(result.schema(), cols))
    }

    pub async fn funnel(&self, ctx: Context, req: Funnel) -> Result<funnel::Response> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = funnel_projection(&ctx, &req, &self.metadata)?;
        let projection = projection
            .iter()
            .map(|x| schema.index_of(x).unwrap())
            .collect();

        let (session_ctx, state, plan) = initial_plan(&self.db, projection).await?;
        let plan = funnel::build(ctx.clone(), self.metadata.clone(), plan, req.clone())?;

        let result = execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);
        let mut group_cols: Vec<StringArray> = vec![];
        let mut ts_col = {
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
                    Breakdown::Property(prop) => {
                        match prop {
                            PropertyRef::System(name) => self.metadata.system_properties.get_by_name(ctx.project_id, name.as_ref())?,
                            PropertyRef::SystemGroup(name) => self.metadata.system_group_properties.get_by_name(ctx.project_id, name.as_ref())?,
                            PropertyRef::Group(name, gid) => self.metadata.group_properties[*gid].get_by_name(ctx.project_id, name.as_ref())?,
                            PropertyRef::Event(name) => self.metadata.event_properties.get_by_name(ctx.project_id, name.as_ref())?,
                            PropertyRef::Custom(_) => return Err(QueryError::Unimplemented("custom properties are not implemented for breakdowns".to_string()))
                        }
                    }
                };
                let col = match result.column(idx + 1).data_type() {
                    DataType::Decimal128(_, _) => {
                        let arr = result.column(idx + 1).as_any().downcast_ref::<Decimal128Array>().unwrap();
                        let mut builder = StringBuilder::new();
                        for i in 0..arr.len() {
                            let v = arr.value(i);
                            let vv = Decimal::from_i128_with_scale(
                                v,
                                DECIMAL_SCALE as u32,
                            ).round_dp(ROUND_DIGITS.into());
                            if vv.is_integer() {
                                builder.append_value(vv.to_i64().unwrap().to_string());
                            } else {
                                builder.append_value(vv.to_string());
                            }
                        }
                        builder.finish().as_any().downcast_ref::<StringArray>().unwrap().clone()
                    }
                    _ => cast(result.column(idx + 1), &DataType::Utf8)?
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .to_owned()
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
            let mut step = funnel::Step {
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
                let step_data = funnel::StepData {
                    groups,
                    ts: ts_col.value(idx),
                    total: int_val_cols[step_id * 4].value(idx) as i64,
                    conversion_ratio: Decimal::from_i128_with_scale(
                        dec_val_cols[step_id * 4].value(idx) as i128,
                        DECIMAL_SCALE as u32,
                    ).round_dp(ROUND_DIGITS.into()),
                    avg_time_to_convert: Decimal::from_i128_with_scale(
                        dec_val_cols[step_id * 4 + 1].value(idx) as i128,
                        DECIMAL_SCALE as u32,
                    ).round_dp(ROUND_DIGITS.into()),
                    avg_time_to_convert_from_start: Decimal::from_i128_with_scale(
                        dec_val_cols[step_id * 4 + 2].value(idx) as i128,
                        DECIMAL_SCALE as u32,
                    ).round_dp(ROUND_DIGITS.into()),
                    dropped_off: int_val_cols[step_id * 4 + 1].value(idx) as i64,
                    drop_off_ratio: Decimal::from_i128_with_scale(
                        dec_val_cols[step_id * 4 + 3].value(idx) as i128,
                        DECIMAL_SCALE as u32,
                    ).round_dp(ROUND_DIGITS.into()),
                    time_to_convert: int_val_cols[step_id * 4 + 2].value(idx) as i64,
                    time_to_convert_from_start: int_val_cols[step_id * 4 + 3].value(idx) as i64,
                };
                step.data.push(step_data);
            }
            steps.push(step);
        }

        Ok(funnel::Response { groups, steps })
    }
}

fn property_values_projection(
    ctx: &Context,
    req: &PropertyValues,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![COLUMN_PROJECT_ID.to_string(), COLUMN_EVENT.to_string()];
    fields.push(col_name(ctx, &req.property, md)?);
    fields.dedup();
    Ok(fields)
}

fn group_records_search_projection(
    ctx: &Context,
    req: &GroupRecordsSearch,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![
        GROUP_COLUMN_PROJECT_ID.to_string(),
        GROUP_COLUMN_ID.to_string(),
        GROUP_COLUMN_CREATED_AT.to_string(),
    ];
    if let Some(filters) = &req.filters {
        for filter in filters {
            match filter {
                PropValueFilter::Property { property, .. } => {
                    fields.push(col_name(ctx, property, md)?)
                }
            }
        }
    }

    for prop in req.properties.clone().unwrap() {
        fields.push(col_name(ctx, &prop, md)?);
    }

    if let Some((prop, ..)) = &req.sort {
        fields.push(col_name(ctx, &prop, md)?);
    }
    Ok(fields)
}

fn event_segmentation_projection(
    ctx: &Context,
    req: &EventSegmentation,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![
        COLUMN_PROJECT_ID.to_string(),
        group_col(req.group_id),
        COLUMN_CREATED_AT.to_string(),
        COLUMN_EVENT.to_string(),
    ];

    for event in &req.events {
        if let Some(filters) = &event.filters {
            for filter in filters {
                match filter {
                    PropValueFilter::Property { property, .. } => {
                        fields.push(col_name(ctx, property, md)?)
                    }
                }
            }
        }

        for query in &event.queries {
            match &query.agg {
                Query::CountEvents => {}
                Query::CountUniqueGroups => {}
                Query::DailyActiveGroups => {}
                Query::WeeklyActiveGroups => {}
                Query::MonthlyActiveGroups => {}
                Query::CountPerGroup { .. } => {}
                Query::AggregatePropertyPerGroup { property, .. } => {
                    fields.push(col_name(ctx, property, md)?)
                }
                Query::AggregateProperty { property, .. } => {
                    fields.push(col_name(ctx, property, md)?)
                }
                Query::QueryFormula { .. } => {}
            }
        }

        if let Some(breakdowns) = &event.breakdowns {
            for breakdown in breakdowns {
                match breakdown {
                    Breakdown::Property(property) => fields.push(col_name(ctx, property, md)?),
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

fn funnel_projection(
    ctx: &Context,
    req: &Funnel,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
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
            if let Some(filters) = &e.event.filters {
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

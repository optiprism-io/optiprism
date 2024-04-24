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
use common::query::event_segmentation::EventSegmentation;
use common::query::event_segmentation::Query;
use common::query::funnel::Funnel;
use common::query::Breakdown;
use common::query::EventFilter;
use common::query::EventRef;
use common::query::PropertyRef;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use common::types::COLUMN_USER_ID;
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
use metadata::MetadataProvider;
use rust_decimal::Decimal;
use storage::db::OptiDBImpl;
use tracing::debug;

use crate::col_name;
use crate::logical_plan::db_parquet::DbParquetNode;
use crate::physical_plan::planner::QueryPlanner;
use crate::queries::event_records_search;
use crate::queries::event_records_search::EventRecordsSearch;
use crate::queries::event_segmentation;
use crate::queries::event_segmentation::logical_plan_builder::COL_AGG_NAME;
use crate::queries::funnel;
use crate::queries::funnel::StepData;
use crate::queries::property_values;
use crate::queries::property_values::PropertyValues;
use crate::Column;
use crate::ColumnType;
use crate::Context;
use crate::DataTable;
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
    pub async fn initial_plan(
        &self,
        projection: Vec<usize>,
    ) -> Result<(SessionContext, SessionState, LogicalPlan)> {
        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionState::new_with_config_rt(
            SessionConfig::new().with_collect_statistics(true),
            runtime,
        )
        .with_query_planner(Arc::new(QueryPlanner {}));

        // if let Some(projection) = projection {
        // state = state
        // .add_physical_optimizer_rule(Arc::new(ApplyParquetProjection::new(projection)));
        // }
        // .with_optimizer_rules(vec![
        // Arc::new(OptimizeProjections::new()),
        // Arc::new(PushDownFilter::new()),
        // ])
        // .with_physical_optimizer_rules(vec![Arc::new(ProjectionPushdown::new())])
        let exec_ctx = SessionContext::new_with_state(state.clone());
        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(DbParquetNode::try_new(self.db.clone(), projection.clone())?),
        });

        Ok((exec_ctx, state, plan))
    }

    pub async fn execute(
        &self,
        ctx: SessionContext,
        state: SessionState,
        plan: LogicalPlan,
    ) -> Result<RecordBatch> {
        let physical_plan = state.create_physical_plan(&plan).await?;
        let displayable_plan = DisplayableExecutionPlan::with_full_metrics(physical_plan.as_ref());
        debug!("physical plan: {}", displayable_plan.indent(true));
        let batches = collect(physical_plan, ctx.task_ctx()).await?;
        let schema: Arc<Schema> = Arc::new(plan.schema().as_ref().into());
        let rows_count = batches.iter().fold(0, |acc, x| acc + x.num_rows());
        let res = concat_batches(&schema, &batches, rows_count)?;

        Ok(res)
    }

    pub async fn property_values(&self, ctx: Context, req: PropertyValues) -> Result<ArrayRef> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = property_values_projection(&ctx, &req, &self.metadata)?;
        let projection = projection
            .iter()
            .map(|x| schema.index_of(x).unwrap())
            .collect();
        let (session_ctx, state, plan) = self.initial_plan(projection).await?;
        let plan = property_values::LogicalPlanBuilder::build(
            ctx,
            self.metadata.clone(),
            plan,
            req.clone(),
        )?;

        let res = self.execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);

        Ok(res.column(0).to_owned())
    }

    pub async fn event_records_search(
        &self,
        ctx: Context,
        req: EventRecordsSearch,
    ) -> Result<DataTable> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = if req.properties.is_some() {
            let projection = event_records_search_projection(&ctx, &req, &self.metadata)?;
            projection
                .iter()
                .map(|x| schema.index_of(x).unwrap())
                .collect()
        } else {
            (0..schema.fields.len()).collect::<Vec<_>>()
        };

        let (session_ctx, state, plan) = self.initial_plan(projection).await?;
        let plan = event_records_search::build(ctx, self.metadata.clone(), plan, req.clone())?;

        let result = self.execute(session_ctx, state, plan).await?;
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
        let (session_ctx, state, plan) = self.initial_plan(projection).await?;
        let plan = event_segmentation::logical_plan_builder::LogicalPlanBuilder::build(
            ctx.clone(),
            self.metadata.clone(),
            plan,
            req.clone(),
        )?;

        println!("{plan:?}");
        let result = self.execute(session_ctx, state, plan).await?;
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

        let (session_ctx, state, plan) = self.initial_plan(projection).await?;
        let plan = funnel::build(ctx.clone(), self.metadata.clone(), plan, req.clone())?;

        let result = self.execute(session_ctx, state, plan).await?;
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
        dbg!(&result);
        if let Some(breakdowns) = &req.breakdowns {
            for idx in 0..breakdowns.len() {
                dbg!(idx);
                let col = result
                    .column(idx + 1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .to_owned();
                group_cols.push(col);
            }
        }
        group_cols.dedup();

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
                        dec_val_cols[step_id * 3].value(idx) as i128,
                        DECIMAL_SCALE as u32,
                    ),
                    avg_time_to_convert: Decimal::from_i128_with_scale(
                        dec_val_cols[step_id * 3 + 1].value(idx) as i128,
                        DECIMAL_SCALE as u32,
                    ),
                    dropped_off: int_val_cols[step_id * 4 + 1].value(idx) as i64,
                    drop_off_ratio: Decimal::from_i128_with_scale(
                        dec_val_cols[step_id * 3 + 2].value(idx) as i128,
                        DECIMAL_SCALE as u32,
                    ),
                    time_to_convert: int_val_cols[step_id * 4 + 2].value(idx) as i64,
                    time_to_convert_from_start: int_val_cols[step_id * 4 + 3].value(idx) as i64,
                };
                step.data.push(step_data);
            }
            steps.push(step);
        }

        Ok(funnel::Response { steps })
    }
}

fn property_values_projection(
    ctx: &Context,
    req: &PropertyValues,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![
        COLUMN_PROJECT_ID.to_string(),
        COLUMN_USER_ID.to_string(),
        COLUMN_EVENT.to_string(),
    ];
    fields.push(col_name(ctx, &req.property, md)?);
    fields.dedup();
    Ok(fields)
}

fn event_records_search_projection(
    ctx: &Context,
    req: &EventRecordsSearch,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![
        COLUMN_PROJECT_ID.to_string(),
        COLUMN_USER_ID.to_string(),
        COLUMN_CREATED_AT.to_string(),
        COLUMN_EVENT.to_string(),
        COLUMN_EVENT_ID.to_string(),
    ];
    if let Some(filters) = &req.filters {
        for filter in filters {
            match filter {
                EventFilter::Property { property, .. } => fields.push(col_name(ctx, property, md)?),
            }
        }
    }

    for prop in req.properties.clone().unwrap() {
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
        COLUMN_USER_ID.to_string(),
        COLUMN_CREATED_AT.to_string(),
        COLUMN_EVENT.to_string(),
    ];

    for event in &req.events {
        if let Some(filters) = &event.filters {
            for filter in filters {
                match filter {
                    EventFilter::Property { property, .. } => {
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
                EventFilter::Property { property, .. } => fields.push(col_name(ctx, property, md)?),
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
        COLUMN_USER_ID.to_string(),
        COLUMN_CREATED_AT.to_string(),
        COLUMN_EVENT.to_string(),
    ];

    for step in &req.steps {
        for event in &step.events {
            if let Some(filters) = &event.filters {
                for filter in filters {
                    match filter {
                        EventFilter::Property { property, .. } => {
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
                        EventFilter::Property { property, .. } => {
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
                EventFilter::Property { property, .. } => fields.push(col_name(ctx, property, md)?),
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

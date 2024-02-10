use std::sync::Arc;
use std::time::Instant;

use arrow::array::ArrayRef;
use arrow::array::StringArray;
use arrow::array::StringBuilder;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use common::query::event_segmentation::Breakdown;
use common::query::event_segmentation::EventSegmentation;
use common::query::event_segmentation::Query;
use common::query::EventFilter;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_PROJECT_ID;
use common::types::COLUMN_USER_ID;

use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::Extension;
use datafusion_expr::LogicalPlan;
use metadata::MetadataProvider;
use storage::db::OptiDBImpl;
use tracing::debug;

use crate::col_name;
use crate::logical_plan::db_parquet::DbParquetNode;
use crate::physical_plan::planner::QueryPlanner;
use crate::queries::event_records_search;
use crate::queries::event_records_search::EventRecordsSearch;
use crate::queries::event_segmentation;
use crate::queries::event_segmentation::logical_plan_builder::COL_AGG_NAME;
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
            SessionConfig::new()
                .with_collect_statistics(true)
                .with_target_partitions(12),
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
        let schema = self.db.schema1("events")?;
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
        let schema = self.db.schema1("events")?;
        let projection = if req.properties.is_some() {
            let projection = event_records_search(&ctx, &req, &self.metadata)?;
            projection
                .iter()
                .map(|x| schema.index_of(x).unwrap())
                .collect()
        } else {
            (0..schema.fields.len()).into_iter().collect::<Vec<_>>()
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
        let schema = self.db.schema1("events")?;
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

        let metric_cols = req.time_columns(ctx.cur_time.clone());
        let cols = result
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let typ = match metric_cols.contains(field.name()) {
                    true => ColumnType::MetricValue,
                    false => ColumnType::Dimension,
                };

                let arr = if field.name() == COL_AGG_NAME {
                    let mut res = StringBuilder::new();
                    let col = result.column(idx);
                    for v in col.as_any().downcast_ref::<StringArray>().unwrap().iter() {
                        let v = v.unwrap();
                        let parts = v.split('_').collect::<Vec<&str>>();
                        let event_id = parts[0].parse::<usize>().unwrap();
                        let query_id = parts[1].parse::<usize>().unwrap();
                        res.append_value(req.events[event_id].queries[query_id].agg.name());
                    }
                    Arc::new(res.finish())
                } else {
                    result.column(idx).to_owned()
                };

                Column {
                    name: field.name().to_owned(),
                    typ,
                    is_nullable: field.is_nullable(),
                    data_type: field.data_type().to_owned(),
                    data: arr,
                }
            })
            .collect();

        Ok(DataTable::new(result.schema(), cols))
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
    fields.push(col_name(ctx, &req.property, &md)?);

    Ok(fields)
}

fn event_records_search(
    ctx: &Context,
    req: &EventRecordsSearch,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![
        COLUMN_PROJECT_ID.to_string(),
        COLUMN_USER_ID.to_string(),
        COLUMN_CREATED_AT.to_string(),
        COLUMN_EVENT.to_string(),
    ];
    if let Some(filters) = &req.filters {
        for filter in filters {
            match filter {
                EventFilter::Property { property, .. } => {
                    fields.push(col_name(ctx, property, &md)?)
                }
            }
        }
    }

    for prop in req.properties.clone().unwrap() {
        fields.push(col_name(ctx, &prop, &md)?);
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
                        fields.push(col_name(ctx, property, &md)?)
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
                    fields.push(col_name(ctx, property, &md)?)
                }
                Query::AggregateProperty { property, .. } => {
                    fields.push(col_name(ctx, property, &md)?)
                }
                Query::QueryFormula { .. } => {}
            }
        }

        if let Some(breakdowns) = &event.breakdowns {
            for breakdown in breakdowns {
                match breakdown {
                    Breakdown::Property(property) => fields.push(col_name(ctx, property, &md)?),
                }
            }
        }
    }

    if let Some(filters) = &req.filters {
        for filter in filters {
            match filter {
                EventFilter::Property { property, .. } => {
                    fields.push(col_name(ctx, property, &md)?)
                }
            }
        }
    }

    if let Some(breakdowns) = &req.breakdowns {
        for breakdown in breakdowns {
            match breakdown {
                Breakdown::Property(property) => fields.push(col_name(ctx, property, &md)?),
            }
        }
    }

    fields.dedup();

    Ok(fields)
}

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use common::query::event_segmentation::Breakdown;
use common::query::event_segmentation::EventSegmentation;
use common::query::event_segmentation::Query;
use common::query::EventFilter;
use common::query::PropertyRef;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_PROJECT_ID;
use common::types::COLUMN_USER_ID;
use datafusion::datasource::DefaultTableSource;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;
use datafusion_expr::LogicalPlanBuilder;
use metadata::MetadataProvider;
use storage::db::OptiDBImpl;
use tracing::debug;

use crate::col_name;
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
    table_source: Arc<DefaultTableSource>,
}

impl QueryProvider {
    pub fn try_new_from_provider(
        metadata: Arc<MetadataProvider>,
        db: Arc<OptiDBImpl>,
        table_provider: Arc<dyn TableProvider>,
    ) -> Result<Self> {
        let table_source = Arc::new(DefaultTableSource::new(table_provider));
        Ok(Self {
            metadata,
            db,
            table_source,
        })
    }
}

impl QueryProvider {
    pub async fn logical_plan(&self) -> Result<(SessionContext, SessionState, LogicalPlan)> {
        let runtime = Arc::new(RuntimeEnv::default());
        let state = SessionState::new_with_config_rt(
            SessionConfig::new()
                .with_collect_statistics(true)
                .with_target_partitions(12),
            runtime,
        )
        .with_query_planner(Arc::new(QueryPlanner {}));
        let exec_ctx = SessionContext::new_with_state(state.clone());
        let opts = ParquetReadOptions::default();
        let plan = exec_ctx
            .read_parquet(self.db.parts_path("events")?, opts)
            .await?
            .into_optimized_plan()?;
        debug!("logical plan: {:?}", plan);

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
        let (session_ctx, state, plan) = self.logical_plan().await?;
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
        let (session_ctx, state, plan) = self.logical_plan().await?;
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
        let (session_ctx, state, plan) = self.logical_plan().await?;
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
                    false => {
                        if field.name() == COL_AGG_NAME {
                            ColumnType::Dimension
                        } else {
                            ColumnType::Metric
                        }
                    }
                };

                Column {
                    name: field.name().to_owned(),
                    typ,
                    is_nullable: field.is_nullable(),
                    data_type: field.data_type().to_owned(),
                    data: result.column(idx).to_owned(),
                }
            })
            .collect();

        Ok(DataTable::new(result.schema(), cols))
    }
}
fn prop_val_fields(
    ctx: &Context,
    req: &PropertyValues,
    md: Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let fields = vec![
        COLUMN_PROJECT_ID.to_string(),
        COLUMN_USER_ID.to_string(),
        COLUMN_CREATED_AT.to_string(),
        COLUMN_EVENT.to_string(),
        col_name(ctx, &req.property, &md)?,
    ];

    Ok(fields)
}

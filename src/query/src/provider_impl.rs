use std::sync::Arc;
use std::time::Instant;

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use chrono::Utc;
use common::query::event_segmentation::EventSegmentation;
use datafusion::datasource::DefaultTableSource;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionState;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;
use metadata::MetadataProvider;
use tracing::debug;

use crate::physical_plan::planner::QueryPlanner;
use crate::queries::event_segmentation;
use crate::queries::event_segmentation::logical_plan_builder::COL_AGG_NAME;
use crate::queries::property_values;
use crate::queries::property_values::PropertyValues;
use crate::Column;
use crate::ColumnType;
use crate::Context;
use crate::DataTable;
use crate::Provider;
use crate::Result;

pub struct ProviderImpl {
    metadata: Arc<MetadataProvider>,
    input: LogicalPlan,
}

impl ProviderImpl {
    pub fn try_new_from_provider(
        metadata: Arc<MetadataProvider>,
        table_provider: Arc<dyn TableProvider>,
    ) -> Result<Self> {
        let table_source = Arc::new(DefaultTableSource::new(table_provider));
        let input =
            datafusion_expr::LogicalPlanBuilder::scan("table", table_source, None)?.build()?;
        Ok(Self { metadata, input })
    }

    pub fn new_from_logical_plan(metadata: Arc<MetadataProvider>, input: LogicalPlan) -> Self {
        Self { metadata, input }
    }
}

#[async_trait]
impl Provider for ProviderImpl {
    async fn property_values(&self, ctx: Context, req: PropertyValues) -> Result<ArrayRef> {
        let plan = property_values::build(&ctx, &self.metadata, self.input.clone(), &req).await?;

        // let plan = LogicalPlanBuilder::from(plan).explain(true, true)?.build()?;

        let result = execute_plan(&plan).await?;

        Ok(result.column(0).to_owned())
    }

    async fn event_segmentation(&self, ctx: Context, es: EventSegmentation) -> Result<DataTable> {
        let cur_time = Utc::now();
        let plan = event_segmentation::logical_plan_builder::build(
            &ctx,
            &self.metadata,
            cur_time,
            self.input.clone(),
            &es,
        )
        .await?;

        // let plan = LogicalPlanBuilder::from(plan).explain(true, true)?.build()?;

        let result = execute_plan(&plan).await?;

        let metric_cols = es.time_columns(cur_time);
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
                            ColumnType::Metric
                        } else {
                            ColumnType::Dimension
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

async fn execute_plan(plan: &LogicalPlan) -> Result<RecordBatch> {
    let start = Instant::now();
    let runtime = Arc::new(RuntimeEnv::default());
    let state = SessionState::with_config_rt(SessionConfig::new(), runtime)
        .with_query_planner(Arc::new(QueryPlanner {}))
        .with_optimizer_rules(vec![]);
    let exec_ctx = SessionContext::with_state(state.clone());
    debug!("logical plan: {:?}", plan);
    let physical_plan = state.create_physical_plan(plan).await?;
    let displayable_plan = displayable(physical_plan.as_ref());

    debug!("physical plan: {}", displayable_plan.indent());
    let batches = collect(physical_plan, exec_ctx.task_ctx()).await?;
    for batch in batches.iter() {
        println!("{}", pretty_format_batches(&[batch.clone()])?);
    }

    let duration = start.elapsed();
    debug!("elapsed: {:?}", duration);
    let schema: Arc<Schema> = Arc::new(plan.schema().as_ref().into());
    let rows_count = batches.iter().fold(0, |acc, x| acc + x.num_rows());

    Ok(concat_batches(&schema, &batches, rows_count)?)
}

use std::sync::Arc;
use arrow::datatypes::Schema;
use chrono::Utc;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::collect;
use datafusion::prelude::{CsvReadOptions, ExecutionConfig, ExecutionContext};
use metadata::database::TableType;
use metadata::Metadata;
use crate::{Context};
use crate::physical_plan::planner::QueryPlanner;
use crate::reports::results::Series;
use crate::Result;
use datafusion::datasource::object_store::local::LocalFileSystem;
use crate::reports::event_segmentation;
use crate::reports::event_segmentation::types::EventSegmentation;

pub struct Provider {
    metadata: Arc<Metadata>,
}

impl Provider {
    pub fn try_new(metadata: Arc<Metadata>) -> Result<Self> {
        Ok(Self {
            metadata,
        })
    }
}

impl Provider {
    pub async fn event_segmentation(&self, ctx: Context, es: EventSegmentation) -> Result<Series> {
        let table = self.metadata.database.get_table(TableType::Events(ctx.organization_id, ctx.project_id)).await?;
        let schema = table.arrow_schema();
        let options = CsvReadOptions::new().schema(&schema);
        let path = "../tests/events.csv";
        let input = datafusion::logical_plan::LogicalPlanBuilder::scan_csv(
            Arc::new(LocalFileSystem {}),
            path,
            options,
            None,
            1,
        )
            .await?
            .build()?;

        let cur_time = Utc::now();
        let plan = event_segmentation::builder::LogicalPlanBuilder::build(ctx, cur_time,self.metadata.clone(), input, es.clone()).await?;
        let config = ExecutionConfig::new().with_query_planner(Arc::new(QueryPlanner {})).with_target_partitions(1);
        let ctx = ExecutionContext::with_config(config);

        let physical_plan = ctx.create_physical_plan(&plan).await?;
        let batches = collect(physical_plan, Arc::new(RuntimeEnv::new(RuntimeConfig::new())?)).await?;
        let schema: Arc<Schema> = Arc::new(plan.schema().as_ref().into());
        let result = concat_batches(&schema, &batches, 0)?;

        let metric_cols = es.time_columns(cur_time);
        let dimension_cols = plan
            .schema()
            .fields()
            .iter()
            .filter_map(|f| match metric_cols.contains(f.name()) {
                true => None,
                false => Some(f.name().clone())
            })
            .collect();

        Ok(Series::try_from_batch_record(&result, dimension_cols, metric_cols)?)
    }
}
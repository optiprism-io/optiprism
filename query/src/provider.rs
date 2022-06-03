use std::sync::Arc;
use std::time::Instant;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use chrono::Utc;
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::datasource::{MemTable, TableProvider};
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
    input: LogicalPlan,
    partitions: usize,
}

impl Provider {
    pub fn try_new(metadata: Arc<Metadata>, schema: SchemaRef, batches: Vec<Vec<RecordBatch>>) -> Result<Self> {
        let partitions = batches.len();
        let input = datafusion::logical_plan::LogicalPlanBuilder::scan_memory(batches, schema, None)?.build()?;
        Ok(Self {
            metadata,
            input,
            partitions,
        })
    }
}

impl Provider {
    pub async fn event_segmentation(&self, ctx: Context, es: EventSegmentation) -> Result<Series> {
        let cur_time = Utc::now();
        let start = Instant::now();
        let plan = event_segmentation::builder::LogicalPlanBuilder::build(
            ctx,
            cur_time.clone(),
            self.metadata.clone(),
            self.input.clone(),
            es.clone()
        ).await?;
        let config = ExecutionConfig::new().with_query_planner(Arc::new(QueryPlanner {}));
        let exec_ctx = ExecutionContext::with_config(config);
        println!("logical plan: {:?}", plan);
        let physical_plan = exec_ctx.create_physical_plan(&plan).await?;
        println!("physical plan: {:?}", physical_plan);
        let batches = collect(physical_plan, Arc::new(RuntimeEnv::new(RuntimeConfig::new())?)).await?;
        let duration = start.elapsed();
        println!("elapsed: {:?}", duration);
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
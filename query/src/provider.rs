use crate::physical_plan::planner::QueryPlanner;
use crate::reports::event_segmentation::logical_plan_builder::COL_AGG_NAME;
use crate::reports::event_segmentation::types::EventSegmentation;
use crate::reports::results::DataTable;
use crate::reports::{event_segmentation, results};
use crate::Context;
use crate::Result;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use chrono::Utc;
use datafusion::catalog::schema::MemorySchemaProvider;
use datafusion::datasource::object_store::local::LocalFileSystem;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_plan::plan::Explain;
use datafusion::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{CsvReadOptions, ExecutionConfig, ExecutionContext};
use metadata::database::TableType;
use metadata::Metadata;
use std::sync::Arc;
use std::time::Instant;

pub struct Provider {
    metadata: Arc<Metadata>,
    input: LogicalPlan,
}

impl Provider {
    pub fn try_new(metadata: Arc<Metadata>, provider: Arc<dyn TableProvider>) -> Result<Self> {
        let input =
            datafusion::logical_plan::LogicalPlanBuilder::scan("table", provider, None)?.build()?;
        Ok(Self { metadata, input })
    }
}

impl Provider {
    pub async fn event_segmentation(
        &self,
        ctx: Context,
        es: EventSegmentation,
    ) -> Result<DataTable> {
        let cur_time = Utc::now();
        let start = Instant::now();
        let plan = event_segmentation::logical_plan_builder::LogicalPlanBuilder::build(
            ctx,
            cur_time.clone(),
            self.metadata.clone(),
            self.input.clone(),
            es.clone(),
        )
        .await?;

        // let plan = LogicalPlanBuilder::from(plan).explain(true, true)?.build()?;

        let config = ExecutionConfig::new()
            .with_query_planner(Arc::new(QueryPlanner {}))
            .with_target_partitions(1);
        let exec_ctx = ExecutionContext::with_config(config);
        println!("logical plan: {:?}", plan);
        let physical_plan = exec_ctx.create_physical_plan(&plan).await?;
        let displayable_plan = displayable(physical_plan.as_ref());

        println!("physical plan: {}", displayable_plan.indent());
        let batches = collect(
            physical_plan,
            Arc::new(RuntimeEnv::new(RuntimeConfig::new())?),
        )
        .await?;
        for batch in batches.iter() {
            println!("{}", pretty_format_batches(&[batch.clone()])?);
        }

        let duration = start.elapsed();
        println!("elapsed: {:?}", duration);
        let schema: Arc<Schema> = Arc::new(plan.schema().as_ref().into());
        let result = concat_batches(&schema, &batches, 0)?;

        let metric_cols = es.time_columns(cur_time);
        let cols = result
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let group = match metric_cols.contains(field.name()) {
                    true => "metric",
                    false => {
                        if field.name() == COL_AGG_NAME {
                            "agg_name"
                        } else {
                            "dimension"
                        }
                    }
                };

                results::Column {
                    name: field.name().to_owned(),
                    group: group.to_string(),
                    is_nullable: field.is_nullable(),
                    data_type: field.data_type().to_owned(),
                    data: result.column(idx).to_owned(),
                }
            })
            .collect();

        Ok(DataTable::new(result.schema(), cols))
    }
}

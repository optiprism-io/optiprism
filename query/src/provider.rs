use crate::data_table::DataTable;
use crate::physical_plan::planner::QueryPlanner;
use crate::queries::event_segmentation::logical_plan_builder::COL_AGG_NAME;
use crate::queries::event_segmentation::types::EventSegmentation;
use crate::queries::{event_segmentation, property_values};
use crate::Result;
use crate::{data_table, Context};
use arrow::datatypes::Schema;

use arrow::util::pretty::pretty_format_batches;
use chrono::Utc;

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};

use datafusion::logical_plan::LogicalPlan;
use datafusion::physical_plan::coalesce_batches::concat_batches;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{ExecutionConfig, ExecutionContext};

use crate::queries::property_values::PropertyValues;
use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use metadata::Metadata;
use std::sync::Arc;
use std::time::Instant;

pub struct QueryProvider {
    metadata: Arc<Metadata>,
    input: LogicalPlan,
}

impl QueryProvider {
    pub fn try_new_from_provider(
        metadata: Arc<Metadata>,
        table_provider: Arc<dyn TableProvider>,
    ) -> Result<Self> {
        let input =
            datafusion::logical_plan::LogicalPlanBuilder::scan("table", table_provider, None)?
                .build()?;
        Ok(Self { metadata, input })
    }

    pub fn new_from_logical_plan(metadata: Arc<Metadata>, input: LogicalPlan) -> Self {
        Self { metadata, input }
    }
}

impl QueryProvider {
    pub async fn property_values(&self, ctx: Context, req: PropertyValues) -> Result<ArrayRef> {
        let plan = property_values::LogicalPlanBuilder::build(
            ctx,
            self.metadata.clone(),
            self.input.clone(),
            req.clone(),
        )
        .await?;

        // let plan = LogicalPlanBuilder::from(plan).explain(true, true)?.build()?;

        let result = execute_plan(&plan).await?;

        Ok(result.column(0).to_owned())
    }

    pub async fn event_segmentation(
        &self,
        ctx: Context,
        es: EventSegmentation,
    ) -> Result<DataTable> {
        let cur_time = Utc::now();
        let plan = event_segmentation::logical_plan_builder::LogicalPlanBuilder::build(
            ctx,
            cur_time,
            self.metadata.clone(),
            self.input.clone(),
            es.clone(),
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
                let group = match metric_cols.contains(field.name()) {
                    true => "metricValue",
                    false => {
                        if field.name() == COL_AGG_NAME {
                            "metric"
                        } else {
                            "dimension"
                        }
                    }
                };

                data_table::Column {
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

async fn execute_plan(plan: &LogicalPlan) -> Result<RecordBatch> {
    let start = Instant::now();

    let config = ExecutionConfig::new().with_query_planner(Arc::new(QueryPlanner {}));
    let exec_ctx = ExecutionContext::with_config(config);
    println!("logical plan: {:?}", plan);
    let physical_plan = exec_ctx.create_physical_plan(plan).await?;
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
    Ok(concat_batches(&schema, &batches, 0)?)
}

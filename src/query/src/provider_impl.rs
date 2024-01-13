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
use datafusion::prelude::SessionConfig;
use datafusion::prelude::SessionContext;
use datafusion_expr::LogicalPlan;
use metadata::MetadataProvider;
use store::db::OptiDBImpl;
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

    //    pub fn new_from_logical_plan(metadata: Arc<MetadataProvider>, input: LogicalPlan) -> Self {
    // Self { metadata, input }
    // }
}

impl QueryProvider {
    pub async fn property_values(&self, ctx: Context, req: PropertyValues) -> Result<ArrayRef> {
        let fields = prop_val_fields(&ctx, &req, self.metadata.clone())?;
        let schema = self.db.schema1("events")?;
        let projection = fields
            .iter()
            .enumerate()
            .map(|(_idx, field)| {
                let idx = schema.index_of(field)?;
                Ok(idx)
            })
            .collect::<Result<Vec<_>>>()?;
        let input = datafusion_expr::LogicalPlanBuilder::scan(
            "table",
            self.table_source.clone(),
            Some(projection),
        )?
        .build()?;

        let plan = property_values::LogicalPlanBuilder::build(
            ctx,
            self.metadata.clone(),
            input,
            req.clone(),
        )
        .await?;

        // let plan = LogicalPlanBuilder::from(plan).explain(true, true)?.build()?;

        let result = execute_plan(&plan).await?;
        // print_batches(&[result.clone()]);
        Ok(result.column(0).to_owned())
    }

    pub async fn event_segmentation(
        &self,
        ctx: Context,
        es: EventSegmentation,
    ) -> Result<DataTable> {
        let cur_time = Utc::now();
        let schema = self.db.schema1("events")?;
        let fields = es_fields(&ctx, &es, self.metadata.clone())?;

        let projection = fields
            .iter()
            .enumerate()
            .map(|(_idx, field)| {
                let idx = schema.index_of(field)?;
                Ok(idx)
            })
            .collect::<Result<Vec<_>>>()?;

        let input = datafusion_expr::LogicalPlanBuilder::scan(
            "table",
            self.table_source.clone(),
            Some(projection),
        )?
        .build()?;
        let plan = event_segmentation::logical_plan_builder::LogicalPlanBuilder::build(
            ctx,
            self.metadata.clone(),
            input,
            es.clone(),
        )?;

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

async fn execute_plan(plan: &LogicalPlan) -> Result<RecordBatch> {
    let start = Instant::now();
    let runtime = Arc::new(RuntimeEnv::default());
    #[allow(deprecated)]
    let state = SessionState::with_config_rt(
        SessionConfig::new()
            .with_collect_statistics(true)
            .with_target_partitions(12),
        runtime,
    )
    .with_query_planner(Arc::new(QueryPlanner {}))
    .with_optimizer_rules(vec![]);
    #[allow(deprecated)]
    let exec_ctx = SessionContext::with_state(state.clone());
    debug!("logical plan: {:?}", plan);
    let physical_plan = state.create_physical_plan(plan).await?;
    let displayable_plan = DisplayableExecutionPlan::with_full_metrics(physical_plan.as_ref());
    debug!("physical plan: {}", displayable_plan.indent(true));
    let batches = collect(physical_plan, exec_ctx.task_ctx()).await?;
    let duration = start.elapsed();
    debug!("elapsed: {:?}", duration);
    let schema: Arc<Schema> = Arc::new(plan.schema().as_ref().into());
    let rows_count = batches.iter().fold(0, |acc, x| acc + x.num_rows());
    let res = concat_batches(&schema, &batches, rows_count)?;
    Ok(res)
}

fn col_name(ctx: &Context, prop: &PropertyRef, md: &Arc<MetadataProvider>) -> Result<String> {
    let name = match prop {
        PropertyRef::System(v) => md
            .system_properties
            .get_by_name(ctx.organization_id, ctx.project_id, v)?
            .column_name(),
        PropertyRef::User(v) => md
            .user_properties
            .get_by_name(ctx.organization_id, ctx.project_id, v)?
            .column_name(),
        PropertyRef::Event(v) => md
            .event_properties
            .get_by_name(ctx.organization_id, ctx.project_id, v)?
            .column_name(),
        _ => unimplemented!(),
    };

    Ok(name)
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
fn es_fields(
    ctx: &Context,
    req: &EventSegmentation,
    md: Arc<MetadataProvider>,
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

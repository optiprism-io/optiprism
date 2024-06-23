use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use arrow::array::ArrayRef;

use common::query::EventRef;
use common::query::PropValueOperation;
use common::query::PropertyRef;
use common::types::{COLUMN_EVENT, COLUMN_PROJECT_ID, TABLE_EVENTS};
use datafusion_common::Column;
use datafusion_common::DFSchema;
use datafusion_common::ScalarValue;
use datafusion_expr::and;
use datafusion_expr::binary_expr;
use datafusion_expr::col;
use datafusion_expr::expr;
use datafusion_expr::lit;
use datafusion_expr::utils::exprlist_to_fields;
use datafusion_expr::Aggregate;
use datafusion_expr::Expr;
use datafusion_expr::Extension;
use datafusion_expr::Filter as PlanFilter;
use datafusion_expr::Limit;
use datafusion_expr::LogicalPlan;
use datafusion_expr::Operator;
use datafusion_expr::Sort;
use tracing::debug;
use metadata::dictionaries::SingleDictionaryProvider;
use metadata::MetadataProvider;
use storage::db::OptiDBImpl;

use crate::error::QueryError;
use crate::error::Result;
use crate::expr::event_expression;
use crate::expr::property_expression;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::{col_name, Context, execute, initial_plan};

pub struct PropertiesProvider {
    metadata: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
}

impl PropertiesProvider {
    pub fn new(metadata: Arc<MetadataProvider>, db: Arc<OptiDBImpl>) -> Self {
        Self { metadata, db }
    }

    pub async fn values(&self, ctx: Context, req: PropertyValues) -> Result<ArrayRef> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = projection(&ctx, &req, &self.metadata)?;
        let projection = projection
            .iter()
            .map(|x| schema.index_of(x).unwrap())
            .collect();
        let (session_ctx, state, plan) = initial_plan(&self.db, TABLE_EVENTS.to_string(), projection).await?;
        let plan = LogicalPlanBuilder::build(
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
}

pub struct LogicalPlanBuilder {}

macro_rules! property_col {
    ($ctx:expr,$md:expr,$tbl:expr,$input:expr,$prop:expr) => {{
        let col_name = $prop.column_name();
        let expr = col(col_name.as_str());
        let _aggr_schema = DFSchema::new_with_metadata(
            exprlist_to_fields(&[expr.clone()], &$input)?,
            HashMap::new(),
        )?;
        let agg_fn = Aggregate::try_new(Arc::new($input.clone()), vec![expr], vec![])?;
        let input = LogicalPlan::Aggregate(agg_fn);

        match $prop.dictionary_type {
            Some(_) => {
                let dict = SingleDictionaryProvider::new(
                    $ctx.project_id,
                    $tbl,
                    col_name.clone(),
                    $md.dictionaries.clone(),
                );

                LogicalPlan::Extension(Extension {
                    node: Arc::new(DictionaryDecodeNode::try_new(input, vec![(
                        Column::from_name(col_name.clone()),
                        Arc::new(dict),
                    )])?),
                })
            }
            None => input,
        }
    }};
}

impl LogicalPlanBuilder {
    pub fn build(
        ctx: Context,
        metadata: Arc<MetadataProvider>,
        input: LogicalPlan,
        req: PropertyValues,
    ) -> Result<LogicalPlan> {
        let mut expr = binary_expr(
            col(COLUMN_PROJECT_ID),
            Operator::Eq,
            lit(ScalarValue::from(ctx.project_id as i64)),
        );

        if let Some(event) = &req.event {
            expr = and(expr, event_expression(&ctx, &metadata, event)?);
        }

        let input = LogicalPlan::Filter(PlanFilter::try_new(expr, Arc::new(input))?);

        let (input, col_name) = match &req.property {
            PropertyRef::System(prop_name) => {
                let prop = metadata
                    .system_properties
                    .get_by_name(ctx.project_id, prop_name)?;
                let col_name = prop.column_name();
                (property_col!(ctx, metadata, TABLE_EVENTS.to_string(),input, prop), col_name)
            }
            PropertyRef::Group(prop_name, group) => {
                let prop =
                    metadata.group_properties[*group].get_by_name(ctx.project_id, prop_name)?;
                let col_name = prop.column_name();
                (property_col!(ctx, metadata, TABLE_EVENTS.to_string(),input, prop), col_name)
            }
            PropertyRef::Event(prop_name) => {
                let prop = metadata
                    .event_properties
                    .get_by_name(ctx.project_id, prop_name)?;
                let col_name = prop.column_name();
                (property_col!(ctx, metadata, TABLE_EVENTS.to_string(),input, prop), col_name)
            }
            _ => {
                return Err(QueryError::Unimplemented(
                    "invalid property type".to_string(),
                ));
            }
        };

        let input = match &req.filter {
            Some(filter) => LogicalPlan::Filter(PlanFilter::try_new(
                property_expression(
                    &ctx,
                    &metadata,
                    TABLE_EVENTS,
                    &req.property,
                    &filter.operation,
                    filter.value.clone(),
                )?,
                Arc::new(input),
            )?),
            None => input,
        };

        let input = LogicalPlan::Sort(Sort {
            expr: vec![Expr::Sort(expr::Sort {
                expr: Box::new(col(col_name.as_str())),
                asc: true,
                nulls_first: false,
            })],
            input: Arc::new(input),
            fetch: None,
        });
        let input = LogicalPlan::Limit(Limit {
            skip: 0,
            fetch: Some(1000),
            input: Arc::new(input),
        });

        Ok(input)
    }
}

#[derive(Clone, Debug)]
pub struct Filter {
    pub operation: PropValueOperation,
    pub value: Option<Vec<ScalarValue>>,
}

#[derive(Clone, Debug)]
pub struct PropertyValues {
    pub property: PropertyRef,
    pub event: Option<EventRef>,
    pub filter: Option<Filter>,
}

fn projection(
    ctx: &Context,
    req: &PropertyValues,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![COLUMN_PROJECT_ID.to_string(), COLUMN_EVENT.to_string()];
    fields.push(col_name(ctx, &req.property, md)?);
    fields.dedup();
    Ok(fields)
}

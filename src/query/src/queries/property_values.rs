use std::collections::HashMap;
use std::sync::Arc;

use common::query::EventRef;
use common::query::PropValueOperation;
use common::query::PropertyRef;
use datafusion_common::Column;
use datafusion_common::DFSchema;
use datafusion_common::ScalarValue;
use datafusion_expr::col;
use datafusion_expr::utils::exprlist_to_fields;
use datafusion_expr::Aggregate;
use datafusion_expr::Expr;
use datafusion_expr::Extension;
use datafusion_expr::Filter as PlanFilter;
use datafusion_expr::LogicalPlan;
use datafusion_expr::Sort;
use metadata::dictionaries::provider_impl::SingleDictionaryProvider;
use metadata::properties::provider_impl::Namespace;
use metadata::MetadataProvider;

use crate::error::Result;
use crate::expr::event_expression;
use crate::expr::property_expression;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::Context;

pub struct LogicalPlanBuilder {}

macro_rules! property_col {
    ($ctx:expr,$md:expr,$input:expr,$prop_name:expr,$md_namespace:ident,$namespace:expr) => {{
        let prop = $md
            .$md_namespace
            .get_by_name($ctx.organization_id, $ctx.project_id, $prop_name)
            .await?;
        let col_name = prop.column_name($namespace);
        let expr = col(col_name.as_str());

        let aggr_schema =
            DFSchema::new_with_metadata(exprlist_to_fields(vec![&expr], &$input)?, HashMap::new())?;
        let expr = LogicalPlan::Aggregate(Aggregate {
            input: Arc::new($input.clone()),
            group_expr: vec![expr],
            aggr_expr: vec![],
            schema: Arc::new(aggr_schema),
        });

        match prop.dictionary_type {
            Some(_) => {
                let dict = SingleDictionaryProvider::new(
                    $ctx.organization_id,
                    $ctx.project_id,
                    col_name.clone(),
                    $md.dictionaries.clone(),
                );

                LogicalPlan::Extension(Extension {
                    node: Arc::new(DictionaryDecodeNode::try_new($input, vec![(
                        Column::from_name(col_name),
                        Arc::new(dict),
                    )])?),
                })
            }
            None => expr,
        }
    }};
}

impl LogicalPlanBuilder {
    pub async fn build(
        ctx: Context,
        metadata: Arc<MetadataProvider>,
        input: LogicalPlan,
        req: PropertyValues,
    ) -> Result<LogicalPlan> {
        let input = match &req.event {
            Some(event) => LogicalPlan::Filter(PlanFilter::try_new(
                event_expression(&ctx, &metadata, event).await?,
                Arc::new(input),
            )?),
            None => input,
        };

        let input = match &req.property {
            PropertyRef::User(prop_name) => {
                property_col!(
                    ctx,
                    metadata,
                    input,
                    prop_name,
                    user_properties,
                    Namespace::User
                )
            }
            PropertyRef::Event(prop_name) => {
                property_col!(
                    ctx,
                    metadata,
                    input,
                    prop_name,
                    event_properties,
                    Namespace::Event
                )
            }
            PropertyRef::Custom(_id) => unimplemented!(),
        };

        let expr_col = input.expressions()[0].clone();

        let input = match &req.filter {
            Some(filter) => LogicalPlan::Filter(PlanFilter::try_new(
                property_expression(
                    &ctx,
                    &metadata,
                    &req.property,
                    &filter.operation,
                    filter.value.clone(),
                )
                .await?,
                Arc::new(input),
            )?),
            None => input,
        };

        let input = LogicalPlan::Sort(Sort {
            expr: vec![Expr::Sort {
                expr: Box::new(expr_col),
                asc: true,
                nulls_first: false,
            }],
            input: Arc::new(input),
            fetch: None,
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

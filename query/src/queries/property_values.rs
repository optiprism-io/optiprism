use crate::error::Result;
use std::sync::Arc;
use arrow::datatypes::DataType;
use chrono::{DateTime, Utc};
use datafusion::logical_plan::LogicalPlan;
use datafusion::logical_plan::plan::{Aggregate, Extension,  Filter as PlanFilter, Sort};
use datafusion::logical_plan::{exprlist_to_fields, DFSchema};
use datafusion_common::{Column, DFField, ScalarValue};
use datafusion_expr::{AggregateFunction, col, Expr, lit, Operator};
use metadata::dictionaries::provider::SingleDictionaryProvider;
use metadata::Metadata;
use metadata::properties::provider::Namespace;
use crate::Context;
use crate::expr::{event_expression, property_expression};
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::queries::types::{EventRef, PropertyRef, PropValueOperation};

pub struct LogicalPlanBuilder {
    ctx: Context,
    cur_time: DateTime<Utc>,
    metadata: Arc<Metadata>,
    req: PropertyValues,
}

macro_rules! property_col {
    ($ctx:expr,$md:expr,$input:expr,$prop_name:expr,$md_namespace:ident,$namespace:expr) => {{
        let prop = $md
                    .$md_namespace
                    .get_by_name($ctx.organization_id, $ctx.project_id, $prop_name)
                    .await?;
                let col_name = prop.column_name($namespace);
                let mut expr = col(col_name.as_str());

                let aggr_schema = DFSchema::new(exprlist_to_fields(vec![&expr], $input.schema())?)?;
                let mut expr = LogicalPlan::Aggregate(Aggregate {
                    input: Arc::new($input.clone()),
                    group_expr: vec![expr],
                    aggr_expr: vec![],
                    schema:Arc::new(aggr_schema),
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
                            node: Arc::new(
                                DictionaryDecodeNode::try_new($input, vec![(Column::from_name(col_name), Arc::new(dict))])?
                            ),
                        })
                    }
                    None => expr
                }
    }}
}

impl LogicalPlanBuilder {
    pub async fn build(
        ctx: Context,
        metadata: Arc<Metadata>,
        input: LogicalPlan,
        req: PropertyValues,
    ) -> Result<LogicalPlan> {
        let mut input = match &req.event {
            Some(event) => LogicalPlan::Filter(PlanFilter {
                predicate: event_expression(&ctx, &metadata, event).await?,
                input: Arc::new((input)),
            }),
            None => input
        };

        let mut input = match &req.property {
            PropertyRef::User(prop_name) => {
                property_col!(ctx,metadata,input,prop_name,user_properties,Namespace::User)
            }
            PropertyRef::Event(prop_name) => {
                property_col!(ctx,metadata,input,prop_name,event_properties,Namespace::Event)
            }
            PropertyRef::Custom(d) => unimplemented!()
        };

        let expr_col = input.expressions()[0].clone();

        input = match &req.filter {
            Some(filter) => {
                LogicalPlan::Filter(PlanFilter {
                    predicate: property_expression(&ctx, &metadata, &req.property, &filter.operation, filter.value.clone()).await?,
                    input: Arc::new((input)),
                })
            }
            None => input
        };

        input = LogicalPlan::Sort(Sort {
            expr: vec![Expr::Sort {
                expr: Box::new(expr_col),
                asc: true,
                nulls_first: false,
            }],
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
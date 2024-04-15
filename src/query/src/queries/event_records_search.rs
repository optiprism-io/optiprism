use std::collections::HashMap;
use std::sync::Arc;

use common::query::EventFilter;
use common::query::EventRef;
use common::query::PropertyRef;
use common::query::QueryTime;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use datafusion_common::Column;
use datafusion_common::ScalarValue;
use datafusion_expr::and;
use datafusion_expr::binary_expr;
use datafusion_expr::col;
use datafusion_expr::expr;
use datafusion_expr::lit;
use datafusion_expr::Expr;
use datafusion_expr::Extension;
use datafusion_expr::Filter as PlanFilter;
use datafusion_expr::Limit;
use datafusion_expr::LogicalPlan;
use datafusion_expr::Operator;
use datafusion_expr::Projection;
use datafusion_expr::Sort;
use metadata::dictionaries::SingleDictionaryProvider;
use metadata::MetadataProvider;

use crate::error::Result;
use crate::expr::event_expression;
use crate::expr::event_filters_expression;
use crate::expr::time_expression;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::expr::multi_and;
use crate::logical_plan::expr::multi_or;
use crate::logical_plan::rename_columns::RenameColumnsNode;
use crate::queries::decode_filter_single_dictionary;
use crate::Context;

pub fn build(
    ctx: Context,
    metadata: Arc<MetadataProvider>,
    input: LogicalPlan,
    req: EventRecordsSearch,
) -> Result<LogicalPlan> {
    let mut properties = vec![];
    let input = if let Some(props) = &req.properties {
        let mut exprs = vec![col(Column {
            relation: None,
            name: COLUMN_PROJECT_ID.to_string(),
        })];
        for prop in props {
            let p = match prop {
                PropertyRef::System(n) => metadata
                    .system_properties
                    .get_by_name(ctx.project_id, n.as_ref())?,
                PropertyRef::User(n) => metadata
                    .user_properties
                    .get_by_name(ctx.project_id, n.as_ref())?,
                PropertyRef::Event(n) => metadata
                    .event_properties
                    .get_by_name(ctx.project_id, n.as_ref())?,
                PropertyRef::Custom(_) => unimplemented!(),
            };
            properties.push(p.clone());
            exprs.push(col(Column {
                relation: None,
                name: p.column_name(),
            }));
        }

        LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(input))?)
    } else {
        input
    };

    let mut cols_hash: HashMap<String, ()> = HashMap::new();
    let input = decode_filter_dictionaries(
        &ctx,
        &metadata,
        req.events.as_ref(),
        req.filters.as_ref(),
        input,
        &mut cols_hash,
    )?;

    let mut filter_exprs = vec![
        binary_expr(
            col(COLUMN_PROJECT_ID),
            Operator::Eq,
            lit(ScalarValue::from(ctx.project_id as i64)),
        ),
        time_expression(COLUMN_CREATED_AT, input.schema(), &req.time, ctx.cur_time)?,
    ];

    if let Some(events) = &req.events
        && !events.is_empty()
    {
        let mut exprs = vec![];
        for event in events {
            // event expression
            let mut expr = event_expression(&ctx, &metadata, &event.event)?;
            // apply event filters
            if let Some(filters) = &event.filters
                && !filters.is_empty()
            {
                expr = and(
                    expr.clone(),
                    event_filters_expression(&ctx, &metadata, filters)?,
                )
            }

            exprs.push(expr);
        }
        filter_exprs.push(multi_or(exprs))
    }

    if let Some(filters) = &req.filters {
        let expr = event_filters_expression(&ctx, &metadata, filters)?;
        filter_exprs = vec![and(filter_exprs[0].clone(), expr)];
    }

    let input = LogicalPlan::Filter(PlanFilter::try_new(
        multi_and(filter_exprs),
        Arc::new(input),
    )?);

    let input = {
        let s = Expr::Sort(expr::Sort {
            expr: Box::new(col(COLUMN_EVENT_ID)),
            asc: false,
            nulls_first: false,
        });

        LogicalPlan::Sort(Sort {
            expr: vec![s],
            input: Arc::new(input),
            fetch: None,
        })
    };

    let input = LogicalPlan::Limit(Limit {
        skip: 0,
        fetch: Some(100),
        input: Arc::new(input),
    });

    if properties.is_empty() {
        let mut l = metadata.system_properties.list(ctx.project_id)?.data;
        properties.append(&mut (l));
        let mut l = metadata.event_properties.list(ctx.project_id)?.data;
        properties.append(&mut (l));
        let mut l = metadata.user_properties.list(ctx.project_id)?.data;
        properties.append(&mut (l));
    }

    let dict_props = properties
        .iter()
        .filter(|prop| prop.is_dictionary && !cols_hash.contains_key(&prop.column_name()))
        .collect::<Vec<_>>();
    let decode_cols = dict_props
        .iter()
        .map(|prop| {
            let col_name = prop.column_name();
            let dict = SingleDictionaryProvider::new(
                ctx.project_id,
                col_name.clone(),
                metadata.dictionaries.clone(),
            );
            let col = Column::from_name(col_name);
            cols_hash.insert(prop.column_name(), ());

            (col, Arc::new(dict))
        })
        .collect::<Vec<_>>();
    let input = if !decode_cols.is_empty() {
        LogicalPlan::Extension(Extension {
            node: Arc::new(DictionaryDecodeNode::try_new(input, decode_cols.clone())?),
        })
    } else {
        input
    };

    let mut rename = vec![];
    for prop in properties {
        let col_name = prop.column_name();
        let new_name = prop.name();
        rename.push((col_name, new_name));
    }
    let input = LogicalPlan::Extension(Extension {
        node: Arc::new(RenameColumnsNode::try_new(input, rename)?),
    });

    Ok(input)
}

fn decode_filter_dictionaries(
    ctx: &Context,
    metadata: &Arc<MetadataProvider>,
    events: Option<&Vec<Event>>,
    filters: Option<&Vec<EventFilter>>,
    input: LogicalPlan,
    cols_hash: &mut HashMap<String, ()>,
) -> Result<LogicalPlan> {
    let mut decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)> = Vec::new();

    if let Some(events) = events {
        for event in events {
            if let Some(filters) = &event.filters {
                for filter in filters {
                    decode_filter_single_dictionary(
                        ctx,
                        metadata,
                        cols_hash,
                        &mut decode_cols,
                        filter,
                    )?;
                }
            }
        }
    }

    if let Some(filters) = filters {
        for filter in filters {
            decode_filter_single_dictionary(ctx, metadata, cols_hash, &mut decode_cols, filter)?;
        }
    }

    if decode_cols.is_empty() {
        return Ok(input);
    }
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(DictionaryDecodeNode::try_new(input, decode_cols.clone())?),
    }))
}

#[derive(Clone, Debug)]
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<EventFilter>>,
}

#[derive(Clone, Debug)]
pub struct EventRecordsSearch {
    pub time: QueryTime,
    pub events: Option<Vec<Event>>,
    pub filters: Option<Vec<EventFilter>>,
    pub properties: Option<Vec<PropertyRef>>,
}

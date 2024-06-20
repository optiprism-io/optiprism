use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common::query::EventRef;
use common::query::PropValueFilter;
use common::query::PropertyRef;
use common::query::QueryTime;
use common::types::{COLUMN_CREATED_AT, COLUMN_EVENT, TABLE_EVENTS};
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use common::GROUPS_COUNT;
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
use tracing::debug;
use metadata::dictionaries::SingleDictionaryProvider;
use metadata::MetadataProvider;
use storage::db::OptiDBImpl;

use crate::error::QueryError;
use crate::error::Result;
use crate::expr::event_expression;
use crate::expr::event_filters_expression;
use crate::expr::time_expression;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::expr::multi_and;
use crate::logical_plan::expr::multi_or;
use crate::logical_plan::rename_columns::RenameColumnsNode;
use crate::queries::{decode_filter_single_dictionary};
use crate::{col_name, ColumnType, Context, DataTable, execute, initial_plan, QueryProvider};

pub struct Provider {
    metadata: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
}

impl Provider {
    pub fn new(metadata: Arc<MetadataProvider>, db: Arc<OptiDBImpl>) -> Self {
        Self { metadata, db }
    }

    pub async fn search(
        &self,
        ctx: Context,
        req: EventRecordsSearch,
    ) -> Result<DataTable> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = if req.properties.is_some() {
            let projection = projection(&ctx, &req, &self.metadata)?;
            projection
                .iter()
                .map(|x| schema.index_of(x).unwrap())
                .collect()
        } else {
            (0..schema.fields.len()).collect::<Vec<_>>()
        };

        let (session_ctx, state, plan) = initial_plan(&self.db, projection).await?;
        let plan = build_plan(ctx, self.metadata.clone(), plan, req.clone())?;
        println!("{plan:?}");
        let result = execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);

        let cols = result
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| crate::Column {
                name: field.name().to_owned(),
                typ: ColumnType::Dimension,
                is_nullable: field.is_nullable(),
                data_type: field.data_type().to_owned(),
                hidden: false,
                data: result.column(idx).to_owned(),
            })
            .collect();

        Ok(DataTable::new(result.schema(), cols))
    }
}

pub fn build_plan(
    ctx: Context,
    metadata: Arc<MetadataProvider>,
    input: LogicalPlan,
    req: EventRecordsSearch,
) -> Result<LogicalPlan> {
    let mut properties = vec![];
    let input = if let Some(props) = &req.properties {
        let mut prop_names = vec![];
        let mut exprs = vec![
            col(Column {
                relation: None,
                name: COLUMN_PROJECT_ID.to_string(),
            }),
            col(Column {
                relation: None,
                name: COLUMN_EVENT_ID.to_string(),
            }),
        ];
        prop_names.push(COLUMN_PROJECT_ID.to_string());
        prop_names.push(COLUMN_EVENT_ID.to_string());

        for prop in props {
            let p = match prop {
                PropertyRef::System(n) => metadata
                    .system_properties
                    .get_by_name(ctx.project_id, n.as_ref())?,
                PropertyRef::Group(n, group_id) => {
                    metadata.group_properties[*group_id].get_by_name(ctx.project_id, n.as_ref())?
                }
                PropertyRef::Event(n) => metadata
                    .event_properties
                    .get_by_name(ctx.project_id, n.as_ref())?,
                _ => {
                    return Err(QueryError::Unimplemented(
                        "invalid property type".to_string(),
                    ));
                }
            };
            if prop_names.contains(&p.column_name()) {
                continue;
            }
            prop_names.push(p.column_name());
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

    // let input = {
    //     let s = Expr::Sort(expr::Sort {
    //         expr: Box::new(col(COLUMN_EVENT_ID)),
    //         asc: false,
    //         nulls_first: false,
    //     });
    //
    //     LogicalPlan::Sort(Sort {
    //         expr: vec![s],
    //         input: Arc::new(input),
    //         fetch: None,
    //     })
    // };

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
        for g in 0..GROUPS_COUNT {
            let mut l = metadata.group_properties[g].list(ctx.project_id)?.data;
            properties.append(&mut (l));
        }
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
    // let input = if !decode_cols.is_empty() {
    //     LogicalPlan::Extension(Extension {
    //         node: Arc::new(DictionaryDecodeNode::try_new(input, decode_cols.clone())?),
    //     })
    // } else {
    //     input
    // };

    let mut rename = vec![];
    let mut rename_found: Vec<String> = vec![];
    for prop in properties {
        let mut found = 0;
        for f in rename_found.iter() {
            if f == &prop.name() {
                found += 1;
            }
        }
        let col_name = prop.column_name();

        let new_name = if found == 0 {
            prop.name()
        } else {
            format!("{} {}", prop.name(), found + 1)
        };
        rename.push((col_name, new_name));
        rename_found.push(prop.name());
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
    filters: Option<&Vec<PropValueFilter>>,
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


fn projection(
    ctx: &Context,
    req: &EventRecordsSearch,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![
        COLUMN_PROJECT_ID.to_string(),
        COLUMN_CREATED_AT.to_string(),
        COLUMN_EVENT.to_string(),
        COLUMN_EVENT_ID.to_string(),
    ];
    if let Some(filters) = &req.filters {
        for filter in filters {
            match filter {
                PropValueFilter::Property { property, .. } => {
                    fields.push(col_name(ctx, property, md)?)
                }
            }
        }
    }

    for prop in req.properties.clone().unwrap() {
        fields.push(col_name(ctx, &prop, md)?);
    }

    Ok(fields)
}

#[derive(Clone, Debug)]
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<PropValueFilter>>,
}

#[derive(Clone, Debug)]
pub struct EventRecordsSearch {
    pub time: QueryTime,
    pub events: Option<Vec<Event>>,
    pub filters: Option<Vec<PropValueFilter>>,
    pub properties: Option<Vec<PropertyRef>>,
}

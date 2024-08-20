use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use arrow::array::{Array, BooleanArray, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMillisecondArray, TimestampMillisecondBuilder};
use arrow::datatypes::DataType;

use common::query::EventRef;
use common::query::PropValueFilter;
use common::query::PropertyRef;
use common::query::QueryTime;
use common::types::{COLUMN_CREATED_AT, COLUMN_EVENT, METRIC_QUERY_EXECUTION_TIME_SECONDS, METRIC_QUERY_QUERIES_TOTAL, TABLE_EVENTS};
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use common::{DECIMAL_PRECISION, DECIMAL_SCALE, group_col, GROUPS_COUNT};
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
use metrics::{counter, histogram};
use tracing::debug;
use metadata::dictionaries::SingleDictionaryProvider;
use metadata::MetadataProvider;
use metadata::properties::{Property, Type};
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
use crate::{col_name, ColumnType, Context, ColumnarDataTable, decode_filter_single_dictionary, execute, initial_plan, PropertyAndValue, fix_filter};

pub struct EventRecordsProvider {
    metadata: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
}

impl EventRecordsProvider {
    pub fn new(metadata: Arc<MetadataProvider>, db: Arc<OptiDBImpl>) -> Self {
        Self { metadata, db }
    }

    pub async fn get_by_id(
        &self,
        ctx: Context,
        id: u64,
    ) -> Result<EventRecord> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection =
            (0..schema.fields.len()).collect::<Vec<_>>();

        let (session_ctx, state, plan) = initial_plan(&self.db, TABLE_EVENTS.to_string(), projection)?;
        let plan = build_get_by_id_plan(&ctx, self.metadata.clone(), plan, id)?;
        println!("{plan:?}");
        let result = execute(session_ctx, state, plan).await?;
        let elapsed = start.elapsed();
        histogram!(METRIC_QUERY_EXECUTION_TIME_SECONDS, "query"=>"event_records_get_by_id").record(elapsed);
        counter!(METRIC_QUERY_QUERIES_TOTAL,"query"=>"event_records_get_by_id").increment(1);
        debug!("elapsed: {:?}", elapsed);

        let mut properties = vec![];

        for (field, col) in result.schema().fields().iter().zip(result.columns().iter()) {
            let mut property = None;
            for p in self.metadata.event_properties.list(ctx.project_id)?.data {
                if p.column_name() == *field.name() {
                    property = Some(p);
                    break;
                }
            };

            if property.is_none() {
                for g in 0..GROUPS_COUNT {
                    for p in self.metadata.group_properties[g].list(ctx.project_id)?.data {
                        if p.column_name() == *field.name() {
                            property = Some(p);
                            break;
                        }
                    };
                }
            }

            if let Some(prop) = property {
                let value = match field.data_type() {
                    DataType::Boolean => {
                        let a = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                        ScalarValue::Boolean(Some(a.value(0)))
                    }
                    DataType::Int8 => {
                        let a = col.as_any().downcast_ref::<Int8Array>().unwrap();
                        ScalarValue::Int8(Some(a.value(0)))
                    }
                    DataType::Int16 => {
                        let a = col.as_any().downcast_ref::<Int16Array>().unwrap();
                        ScalarValue::Int16(Some(a.value(0).to_owned()))
                    }
                    DataType::Int32 => {
                        let a = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        ScalarValue::Int32(Some(a.value(0).to_owned()))
                    }
                    DataType::Int64 => {
                        let a = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        ScalarValue::Int64(Some(a.value(0).to_owned()))
                    }
                    DataType::Utf8 => {
                        {
                            let a = col.as_any().downcast_ref::<StringArray>().unwrap();
                            ScalarValue::Utf8(Some(a.value(0).to_owned()))
                        }
                    }
                    DataType::Decimal128(_, _) => {
                        let a = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                        ScalarValue::Decimal128(Some(a.value(0)), DECIMAL_PRECISION, DECIMAL_SCALE)
                    }
                    DataType::Timestamp(tu, v) => {
                        let a = col.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                        ScalarValue::TimestampMillisecond(Some(a.value(0)), None).to_owned()
                    }
                    _ => unimplemented!("unimplemented {}", field.data_type().to_string())
                };

                properties.push(PropertyAndValue { property: prop.reference(), value })
            }
        }
        let rec = EventRecord { properties };

        Ok(rec)
    }

    pub async fn search(
        &self,
        ctx: Context,
        req: EventRecordsSearchRequest,
    ) -> Result<ColumnarDataTable> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let mut proj = if req.properties.is_some() {
            let p = projection(&ctx, &req, &self.metadata)?;
            p
                .iter()
                .map(|x| schema.index_of(x).unwrap())
                .collect()
        } else {
            (0..schema.fields.len()).collect::<Vec<_>>()
        };
        proj.sort();
        proj.dedup();

        // todo make tests for schema evolution: add property here. Move initial plan behind build_search_plan
        let (session_ctx, state, plan) = initial_plan(&self.db, TABLE_EVENTS.to_string(), proj)?;
        let (plan, props) = build_search_plan(ctx, self.metadata.clone(), plan, req.clone())?;
        println!("{plan:?}");
        let result = execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);
        let elapsed = start.elapsed();
        histogram!(METRIC_QUERY_EXECUTION_TIME_SECONDS, "query"=>"event_records_search").record(elapsed);
        counter!(METRIC_QUERY_QUERIES_TOTAL,"query"=>"event_records_search").increment(1);
        debug!("elapsed: {:?}", elapsed);

        let cols = result
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let prop = &props[idx];
                crate::Column {
                    property: Some(prop.reference()),
                    name: field.name().to_owned(),
                    typ: ColumnType::Dimension,
                    is_nullable: field.is_nullable(),
                    data_type: field.data_type().to_owned(),
                    hidden: false,
                    data: result.column(idx).to_owned(),
                }
            })
            .collect();

        Ok(ColumnarDataTable::new(result.schema(), cols))
    }
}

pub fn build_search_plan(
    ctx: Context,
    metadata: Arc<MetadataProvider>,
    input: LogicalPlan,
    req: EventRecordsSearchRequest,
) -> Result<(LogicalPlan, Vec<Property>)> {
    let mut properties = vec![];
    // todo: add events and filters
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
            col(Column {
                relation: None,
                name: COLUMN_CREATED_AT.to_string(),
            }),
        ];
        prop_names.push(COLUMN_PROJECT_ID.to_string());
        prop_names.push(COLUMN_CREATED_AT.to_string());
        prop_names.push(COLUMN_EVENT_ID.to_string());
        let p = metadata.event_properties.get_by_column_name(ctx.project_id, COLUMN_PROJECT_ID)?;
        properties.push(p);
        let p = metadata.event_properties.get_by_column_name(ctx.project_id, COLUMN_EVENT_ID)?;
        properties.push(p);
        let p = metadata.event_properties.get_by_column_name(ctx.project_id, COLUMN_CREATED_AT)?;
        properties.push(p);
        for prop in props {
            let p = match prop {
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
            let dict = match prop.typ {
                Type::Event => {
                    SingleDictionaryProvider::new(
                        ctx.project_id,
                        TABLE_EVENTS.to_string(),
                        col_name.clone(),
                        metadata.dictionaries.clone(),
                    )
                }
                Type::Group(g) => {
                    SingleDictionaryProvider::new(
                        ctx.project_id,
                        group_col(g),
                        col_name.clone(),
                        metadata.dictionaries.clone(),
                    )
                }
            };
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
    let mut rename_found: Vec<String> = vec![];
    for prop in &properties {
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

    Ok((input, properties))
}

pub fn build_get_by_id_plan(
    ctx: &Context,
    metadata: Arc<MetadataProvider>,
    input: LogicalPlan,
    id: u64,
) -> Result<LogicalPlan> {
    let mut filter_exprs = vec![
        binary_expr(
            col(COLUMN_PROJECT_ID),
            Operator::Eq,
            lit(ScalarValue::from(ctx.project_id as i64)),
        ),
        binary_expr(
            col(COLUMN_EVENT_ID),
            Operator::Eq,
            lit(ScalarValue::from(id as i64)),
        ),
    ];

    let input = LogicalPlan::Filter(PlanFilter::try_new(
        multi_and(filter_exprs),
        Arc::new(input),
    )?);

    let mut properties = vec![];
    let mut l = metadata.event_properties.list(ctx.project_id)?.data;
    properties.append(&mut (l));
    for g in 0..GROUPS_COUNT {
        let mut l = metadata.group_properties[g].list(ctx.project_id)?.data;
        properties.append(&mut (l));
    }

    let mut cols_hash: HashMap<String, ()> = HashMap::new();
    let dict_props = properties
        .iter()
        .filter(|prop| prop.is_dictionary && !cols_hash.contains_key(&prop.column_name()))
        .collect::<Vec<_>>();
    let decode_cols = dict_props
        .iter()
        .map(|prop| {
            let col_name = prop.column_name();
            let dict = match prop.typ {
                Type::Event => {
                    SingleDictionaryProvider::new(
                        ctx.project_id,
                        TABLE_EVENTS.to_string(),
                        col_name.clone(),
                        metadata.dictionaries.clone(),
                    )
                }
                Type::Group(g) => {
                    SingleDictionaryProvider::new(
                        ctx.project_id,
                        group_col(g).to_string(),
                        col_name.clone(),
                        metadata.dictionaries.clone(),
                    )
                }
            };
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
    req: &EventRecordsSearchRequest,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![
        COLUMN_PROJECT_ID.to_string(),
        COLUMN_EVENT.to_string(),
        COLUMN_CREATED_AT.to_string(),
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

pub fn fix_search_request(
    md: &Arc<MetadataProvider>,
    project_id: u64,
    req: EventRecordsSearchRequest,
) -> Result<EventRecordsSearchRequest> {
    let mut out = req.clone();

    if let Some(props) = &req.properties {
        if props.is_empty() {
            out.properties = None;
        }
    }

    if let Some(events) = req.events {
        let mut oe = vec![];
        for event in events.iter() {
            let mut of = vec![];
            if let Some(filters) = &event.filters {
                if filters.is_empty() {} else {
                    for filter in filters.iter() {
                        let f = fix_filter(md, project_id, filter)?;
                        of.push(f);
                    }
                }
                let e = Event {
                    event: event.event.clone(),
                    filters: if of.is_empty() { None } else { Some(of) },
                };
                oe.push(e);
            }
        }
        out.events = Some(oe);
    }


    if let Some(filters) = &req.filters {
        if filters.is_empty() {
            out.filters = None;
        } else {
            let mut filters_out = vec![];
            for filter in filters.iter() {
                let f = fix_filter(md, project_id, filter)?;
                filters_out.push(f);
            }
            out.filters = Some(filters_out);
        }
    }

    if let Some(properties) = &req.properties {
        if properties.is_empty() {
            out.properties = None;
        }
    }
    Ok(out)
}

#[derive(Clone, Debug)]
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<PropValueFilter>>,
}

#[derive(Clone, Debug)]
pub struct EventRecord {
    pub properties: Vec<PropertyAndValue>,
}

#[derive(Clone, Debug)]
pub struct EventRecordsSearchRequest {
    pub time: QueryTime,
    pub events: Option<Vec<Event>>,
    pub filters: Option<Vec<PropValueFilter>>,
    pub properties: Option<Vec<PropertyRef>>,
}

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use arrow::array::{Array, BooleanArray, Decimal128Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMillisecondArray, TimestampMillisecondBuilder};
use arrow::datatypes::DataType;
use arrow::util::pretty::print_batches;

use common::query::EventRef;
use common::query::PropValueFilter;
use common::query::PropertyRef;
use common::query::QueryTime;
use common::types::{COLUMN_CREATED_AT, COLUMN_EVENT, GROUP_COLUMN_CREATED_AT, GROUP_COLUMN_ID, GROUP_COLUMN_PROJECT_ID, GROUP_COLUMN_VERSION, METRIC_QUERY_EXECUTION_TIME_MS, SortDirection, TABLE_EVENTS};
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
use metrics::histogram;
use tracing::debug;
use metadata::dictionaries::SingleDictionaryProvider;
use metadata::MetadataProvider;
use metadata::properties::Property;
use storage::db::OptiDBImpl;

use crate::error::QueryError;
use crate::error::Result;
use crate::expr::{event_expression, property_col};
use crate::expr::event_filters_expression;
use crate::expr::time_expression;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::expr::multi_and;
use crate::logical_plan::expr::multi_or;
use crate::logical_plan::rename_columns::RenameColumnsNode;
use crate::{col_name, ColumnType, Context, ColumnarDataTable, decode_filter_single_dictionary, execute, initial_plan, PropertyAndValue};

pub struct GroupRecordsProvider {
    metadata: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
}

impl GroupRecordsProvider {
    pub fn new(metadata: Arc<MetadataProvider>, db: Arc<OptiDBImpl>) -> Self {
        Self { metadata, db }
    }

    pub async fn get_by_id(
        &self,
        ctx: Context,
        group_id: usize,
        id: u64,
    ) -> Result<GroupRecord> {
        let start = Instant::now();
        let schema = self.db.schema1(&group_col(group_id))?;
        let projection =
            (0..schema.fields.len()).collect::<Vec<_>>();

        let (session_ctx, state, plan) = initial_plan(&self.db, group_col(group_id), projection)?;
        let plan = build_get_by_id_plan(&ctx, self.metadata.clone(), plan, group_id, id)?;
        println!("{plan:?}");
        let result = execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);
        let elapsed = start.elapsed();
        histogram!(METRIC_QUERY_EXECUTION_TIME_MS, "query"=>"group_records_get_by_id").record(elapsed);
        debug!("elapsed: {:?}", elapsed);
        let mut properties = vec![];

        for (field, col) in result.schema().fields().iter().zip(result.columns().iter()) {
            let mut property = None;
            for p in self.metadata.group_properties[group_id].list(ctx.project_id)?.data {
                if p.column_name() == *field.name() {
                    property = Some(p);
                    break;
                }
            };

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
        let rec = GroupRecord { properties };

        Ok(rec)
    }

    pub async fn search(
        &self,
        ctx: Context,
        req: GroupRecordsSearchRequest,
    ) -> Result<ColumnarDataTable> {
        let start = Instant::now();
        let schema = self.db.schema1(group_col(req.group_id).as_str())?;
        let projection = if req.properties.is_some() {
            let projection = projection(&ctx, &req, &self.metadata)?;
            projection
                .iter()
                .map(|x| schema.index_of(x).unwrap())
                .collect()
        } else {
            (0..schema.fields.len()).collect::<Vec<_>>()
        };

        let (session_ctx, state, plan) = initial_plan(&self.db, group_col(req.group_id), projection)?;
        let (plan,props) = build_search_plan(ctx, self.metadata.clone(), plan, req.clone())?;
        println!("{plan:?}");
        let result = execute(session_ctx, state, plan).await?;

        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);
        let elapsed = start.elapsed();
        histogram!(METRIC_QUERY_EXECUTION_TIME_MS, "query"=>"event_records_search").record(elapsed);
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
    req: GroupRecordsSearchRequest,
) -> Result<(LogicalPlan,Vec<Property>)> {
    let mut properties = vec![];
    let input = if let Some(props) = &req.properties {
        let mut prop_names = vec![];
        let mut exprs = vec![
            col(Column {
                relation: None,
                name: GROUP_COLUMN_PROJECT_ID.to_string(),
            }),
            col(Column {
                relation: None,
                name: GROUP_COLUMN_ID.to_string(),
            }),
            col(Column {
                relation: None,
                name: GROUP_COLUMN_VERSION.to_string(),
            }),
            col(Column {
                relation: None,
                name: GROUP_COLUMN_CREATED_AT.to_string(),
            }),
        ];
        prop_names.push(GROUP_COLUMN_PROJECT_ID.to_string());
        prop_names.push(GROUP_COLUMN_ID.to_string());
        prop_names.push(GROUP_COLUMN_VERSION.to_string());
        prop_names.push(GROUP_COLUMN_CREATED_AT.to_string());

        for prop in props {
            let p = match prop {
                PropertyRef::Group(n, group_id) => {
                    metadata.group_properties[*group_id].get_by_name(ctx.project_id, n.as_ref())?
                }
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
    let input =
        decode_filter_dictionaries(&ctx, &metadata, req.filters.as_ref(), input, &mut cols_hash)?;

    let mut filter_exprs = vec![binary_expr(
        col(GROUP_COLUMN_PROJECT_ID),
        Operator::Eq,
        lit(ScalarValue::from(ctx.project_id as i64)),
    )];

    if let Some(time) = &req.time {
        let expr = time_expression(GROUP_COLUMN_CREATED_AT, input.schema(), time, ctx.cur_time)?;
        filter_exprs.push(expr);
    }

    if let Some(filters) = &req.filters {
        let expr = event_filters_expression(&ctx, &metadata, filters)?;
        filter_exprs = vec![and(filter_exprs[0].clone(), expr)];
    }

    let input = LogicalPlan::Filter(PlanFilter::try_new(
        multi_and(filter_exprs),
        Arc::new(input),
    )?);

    let input = if let Some((prop, sort)) = &req.sort {
        let s = Expr::Sort(expr::Sort {
            expr: Box::new(property_col(&ctx, &metadata, prop)?),
            asc: if *sort == SortDirection::Asc {
                true
            } else {
                false
            },
            nulls_first: false,
        });

        LogicalPlan::Sort(Sort {
            expr: vec![s],
            input: Arc::new(input),
            fetch: None,
        })
    } else {
        input
    };

    let input = LogicalPlan::Limit(Limit {
        skip: 0,
        fetch: Some(1000),
        input: Arc::new(input),
    });

    if properties.is_empty() {
        let mut l = metadata.group_properties[req.group_id]
            .list(ctx.project_id)?
            .data;
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
                group_col(req.group_id),
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

    Ok((input,properties))
}

pub fn build_get_by_id_plan(
    ctx: &Context,
    metadata: Arc<MetadataProvider>,
    input: LogicalPlan,
    group_id: usize,
    id: u64,
) -> Result<LogicalPlan> {
    let mut filter_exprs = vec![
        binary_expr(
            col(GROUP_COLUMN_PROJECT_ID),
            Operator::Eq,
            lit(ScalarValue::from(ctx.project_id as i64)),
        ),
        binary_expr(
            col(GROUP_COLUMN_ID),
            Operator::Eq,
            lit(ScalarValue::from(id as i64)),
        ),
    ];

    let input = LogicalPlan::Filter(PlanFilter::try_new(
        multi_and(filter_exprs),
        Arc::new(input),
    )?);

    let mut properties = vec![];
    let mut l = metadata.group_properties[group_id].list(ctx.project_id)?.data;
    properties.append(&mut (l));

    let mut cols_hash: HashMap<String, ()> = HashMap::new();
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
                group_col(group_id),
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

    Ok(input)
}

fn decode_filter_dictionaries(
    ctx: &Context,
    metadata: &Arc<MetadataProvider>,
    filters: Option<&Vec<PropValueFilter>>,
    input: LogicalPlan,
    cols_hash: &mut HashMap<String, ()>,
) -> Result<LogicalPlan> {
    let mut decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)> = Vec::new();

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
    req: &GroupRecordsSearchRequest,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![
        GROUP_COLUMN_PROJECT_ID.to_string(),
        group_col(req.group_id).to_string(),
        GROUP_COLUMN_VERSION.to_string(),
        GROUP_COLUMN_CREATED_AT.to_string(),
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
pub struct GroupRecord {
    pub properties: Vec<PropertyAndValue>,
}

#[derive(Clone, Debug)]
pub struct GroupRecordsSearchRequest {
    pub time: Option<QueryTime>,
    pub group_id: usize,
    pub filters: Option<Vec<PropValueFilter>>,
    pub properties: Option<Vec<PropertyRef>>,
    pub sort: Option<(PropertyRef, SortDirection)>,
}

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common::group_col;
use common::event_segmentation::Event;
use common::event_segmentation::EventSegmentationRequest;
use common::event_segmentation::Query;
use common::query::{PropValueFilter, time_columns};
use common::query::Breakdown;
use common::query::DidEventAggregate;
use common::query::PropertyRef;
use common::query::SegmentCondition;
use common::types::{COLUMN_CREATED_AT, COLUMN_IP, GROUP_COLUMN_ID, TABLE_EVENTS};
use common::types::COLUMN_EVENT;
use common::types::COLUMN_PROJECT_ID;
use common::types::COLUMN_SEGMENT;
use datafusion::functions::datetime::date_trunc::DateTruncFunc;
use datafusion::functions::math::trunc::TruncFunc;
use datafusion_common::Column;
use datafusion_common::ScalarValue;
use datafusion_expr::binary_expr;
use datafusion_expr::col;
use datafusion_expr::expr;
use datafusion_expr::expr::Alias;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_fn::and;
use datafusion_expr::lit;
use datafusion_expr::Expr;
use datafusion_expr::ExprSchemable;
use datafusion_expr::Extension;
use datafusion_expr::Filter;
use datafusion_expr::LogicalPlan;
use datafusion_expr::Operator;
use datafusion_expr::ScalarUDF;
use datafusion_expr::Sort;
use tracing::debug;
use metadata::dictionaries::SingleDictionaryProvider;
use metadata::MetadataProvider;
use storage::db::OptiDBImpl;

use crate::{breakdowns_to_dicts, col_name, ColumnType, ColumnarDataTable, decode_filter_single_dictionary, execute, initial_plan, segment_projection, segment_plan};
use crate::context::Format;
use crate::error::QueryError;
use crate::error::Result;
use crate::expr::breakdown_expr;
use crate::expr::event_expression;
use crate::expr::event_filters_expression;
use crate::expr::property_col;
use crate::expr::property_expression;
use crate::expr::time_expression;
use crate::logical_plan::add_string_column::AddStringColumnNode;
use crate::logical_plan::aggregate_columns::AggregateAndSortColumnsNode;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::limit_groups::LimitGroupsNode;
use crate::logical_plan::merge::MergeNode;
use crate::logical_plan::partitioned_aggregate;
use crate::logical_plan::partitioned_aggregate::AggregateExpr;
use crate::logical_plan::partitioned_aggregate::PartitionedAggregateFinalNode;
use crate::logical_plan::partitioned_aggregate::PartitionedAggregatePartialNode;
use crate::logical_plan::pivot::PivotNode;
use crate::logical_plan::rename_column_rows::RenameColumnRowsNode;
use crate::logical_plan::rename_columns::RenameColumnsNode;
use crate::logical_plan::reorder_columns::ReorderColumnsNode;
use crate::logical_plan::segment;
use crate::logical_plan::segment::SegmentExpr;
use crate::logical_plan::segment::SegmentNode;
use crate::logical_plan::unpivot::UnpivotNode;
use crate::logical_plan::SortField;
use crate::Context;

pub const COL_AGG_NAME: &str = "agg_name";
const COL_VALUE: &str = "value";

pub struct EventSegmentationProvider {
    metadata: Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
}

impl EventSegmentationProvider {
    pub fn new(metadata: Arc<MetadataProvider>, db: Arc<OptiDBImpl>) -> Self {
        Self { metadata, db }
    }
    pub async fn event_segmentation(
        &self,
        ctx: Context,
        req: EventSegmentationRequest,
    ) -> Result<ColumnarDataTable> {
        let start = Instant::now();
        let schema = self.db.schema1(TABLE_EVENTS)?;
        let projection = projection(&ctx, &req, &self.metadata)?;
        let projection = projection
            .iter()
            .map(|x| schema.index_of(x).unwrap())
            .collect();
        let (session_ctx, state, plan) = initial_plan(&self.db, TABLE_EVENTS.to_string(), projection)?;
        let segment_plan = if let Some(segments) = &req.segments {
            let segment_projection = segment_projection(&ctx, segments, req.group_id, &self.metadata)?;
            let segment_projection = segment_projection
                .iter()
                .map(|x| schema.index_of(x).unwrap())
                .collect();
            Some(segment_plan(&self.db, TABLE_EVENTS.to_string(), segment_projection)?)
        } else {
            None
        };


        let plan = LogicalPlanBuilder::build(
            ctx.clone(),
            self.metadata.clone(),
            plan,
            segment_plan,
            req.clone(),
        )?;

        println!("{plan:?}");
        let result = execute(session_ctx, state, plan).await?;
        let duration = start.elapsed();
        debug!("elapsed: {:?}", duration);

        let metric_cols = req.time_columns(ctx.cur_time);
        let cols = result
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let typ = match metric_cols.contains(field.name()) {
                    true => ColumnType::Metric,
                    false => ColumnType::Dimension,
                };

                let arr = result.column(idx).to_owned();

                crate::Column {
                    property: None,
                    name: field.name().to_owned(),
                    typ,
                    is_nullable: field.is_nullable(),
                    data_type: field.data_type().to_owned(),
                    hidden: false,
                    data: arr,
                }
            })
            .collect();

        Ok(ColumnarDataTable::new(result.schema(), cols))
    }
}

fn projection(
    ctx: &Context,
    req: &EventSegmentationRequest,
    md: &Arc<MetadataProvider>,
) -> Result<Vec<String>> {
    let mut fields = vec![
        COLUMN_PROJECT_ID.to_string(),
        group_col(req.group_id),
        COLUMN_CREATED_AT.to_string(),
        COLUMN_EVENT.to_string(),
    ];

    for event in &req.events {
        if let Some(filters) = &event.filters {
            for filter in filters {
                match filter {
                    PropValueFilter::Property { property, .. } => {
                        fields.push(col_name(ctx, property, md)?)
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
                    fields.push(col_name(ctx, property, md)?)
                }
                Query::AggregateProperty { property, .. } => {
                    fields.push(col_name(ctx, property, md)?)
                }
                Query::QueryFormula { .. } => {}
            }
        }

        if let Some(breakdowns) = &event.breakdowns {
            for breakdown in breakdowns {
                match breakdown {
                    Breakdown::Property(property) => fields.push(col_name(ctx, property, md)?),
                }
            }
        }
    }

    if let Some(filters) = &req.filters {
        for filter in filters {
            match filter {
                PropValueFilter::Property { property, .. } => {
                    fields.push(col_name(ctx, property, md)?)
                }
            }
        }
    }

    if let Some(breakdowns) = &req.breakdowns {
        for breakdown in breakdowns {
            match breakdown {
                Breakdown::Property(property) => fields.push(col_name(ctx, property, md)?),
            }
        }
    }

    fields.dedup();

    Ok(fields)
}

pub struct LogicalPlanBuilder {
    ctx: Context,
    metadata: Arc<MetadataProvider>,
    es: EventSegmentationRequest,
}

impl LogicalPlanBuilder {
    /// creates logical plan for event segmentation
    pub fn build(
        ctx: Context,
        metadata: Arc<MetadataProvider>,
        input: LogicalPlan,
        segment_input: Option<LogicalPlan>,
        es: EventSegmentationRequest,
    ) -> Result<LogicalPlan> {
        let events = es.events.clone();
        let builder = LogicalPlanBuilder {
            ctx: ctx.clone(),
            metadata: metadata.clone(),
            es: es.clone(),
        };

        let segment_inputs = if let Some(segments) = es.segments.clone() {
            let mut inputs = Vec::new();
            for segment in segments {
                let mut or: Option<SegmentExpr> = None;
                for conditions in segment.conditions {
                    let mut and: Option<SegmentExpr> = None;
                    for condition in conditions {
                        let expr = builder.build_segment_condition(&condition, es.group_id)?;
                        and = match and {
                            None => Some(expr),
                            Some(e) => Some(SegmentExpr::And(Box::new(e), Box::new(expr))),
                        };
                    }

                    or = match or {
                        None => Some(and.unwrap()),
                        Some(e) => Some(SegmentExpr::Or(Box::new(e), Box::new(and.unwrap()))),
                    };
                }

                let node = SegmentNode::try_new(
                    input.clone(),
                    or.unwrap(),
                    Column::from_qualified_name(group_col(es.group_id)),
                )?;
                let input = LogicalPlan::Extension(Extension {
                    node: Arc::new(node),
                });

                inputs.push(input);
            }
            Some(inputs)
        } else {
            None
        };

        let mut cols_hash: HashMap<String, ()> = HashMap::new();

        let mut input = builder.decode_filter_dictionaries(input, &mut cols_hash)?;
        // build main query
        input = match events.len() {
            1 => {
                let input = builder.build_event_logical_plan(
                    input.clone(),
                    0,
                    es.group_id,
                    segment_inputs,
                )?;

                if ctx.format != Format::Compact {
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(AddStringColumnNode::try_new(
                            input,
                            ("event".to_string(), events[0].event.name()),
                        )?),
                    })
                } else {
                    input
                }
            }
            _ => {
                let mut inputs: Vec<LogicalPlan> = vec![];
                for idx in 0..events.len() {
                    let input = builder.build_event_logical_plan(
                        input.clone(),
                        idx,
                        es.group_id,
                        segment_inputs.clone(),
                    )?;

                    inputs.push(input);
                }
                let event_names = events
                    .iter()
                    .map(|event| event.event.name())
                    .collect::<Vec<_>>();

                // merge multiple results into one schema
                LogicalPlan::Extension(Extension {
                    node: Arc::new(MergeNode::try_new(
                        inputs,
                        Some(("event".to_string(), event_names)),
                    )?),
                })
            }
        };

        input = builder.decode_breakdowns_dictionaries(input, TABLE_EVENTS, &mut cols_hash)?;

        if ctx.format == Format::Compact {
            return Ok(input);
        }
        let mut group_cols = if ctx.format != Format::Compact {
            vec![COLUMN_EVENT.to_string(), COLUMN_SEGMENT.to_string()]
        } else {
            vec![COLUMN_SEGMENT.to_string()]
        };

        let mut display_group_cols: Vec<String> = vec![];

        let mut rename_groups = if ctx.format != Format::Compact {
            vec![
                (COLUMN_EVENT.to_string(), "Event".to_string()),
                (COLUMN_SEGMENT.to_string(), "Segment".to_string()),
            ]
        } else {
            vec![(COLUMN_SEGMENT.to_string(), "Segment".to_string())]
        };

        for event in &es.events {
            if let Some(breakdowns) = &event.breakdowns {
                for breakdown in breakdowns.iter() {
                    let prop = match breakdown {
                        Breakdown::Property(p) => match p {
                            PropertyRef::Group(p, group) => {
                                metadata.group_properties[*group].get_by_name(ctx.project_id, p)?
                            }
                            PropertyRef::Event(p) => {
                                metadata.event_properties.get_by_name(ctx.project_id, p)?
                            }
                            _ => {
                                return Err(QueryError::Unimplemented(
                                    "invalid property type".to_string(),
                                ));
                            }
                        },
                    };
                    let cn = prop.column_name();
                    if !group_cols.contains(&cn) {
                        group_cols.push(cn.clone());
                        let mut found = 0;
                        for v in display_group_cols.iter() {
                            if *v == prop.name() {
                                found += 1;
                            }
                        }
                        display_group_cols.push(prop.name());
                        if found == 0 {
                            rename_groups.push((cn, prop.name()));
                        } else {
                            rename_groups.push((cn, format!("{} {}", prop.name(), found + 1)));
                        }
                    }
                }
            }
        }

        if let Some(breakdowns) = &es.breakdowns {
            for breakdown in breakdowns.iter() {
                let prop = match breakdown {
                    Breakdown::Property(p) => match p {
                        PropertyRef::Group(p, group) => {
                            metadata.group_properties[*group].get_by_name(ctx.project_id, p)?
                        }
                        PropertyRef::Event(p) => {
                            metadata.event_properties.get_by_name(ctx.project_id, p)?
                        }
                        _ => {
                            return Err(QueryError::Unimplemented(
                                "invalid property type".to_string(),
                            ));
                        }
                    },
                };
                let cn = prop.column_name();
                if !group_cols.contains(&cn) {
                    group_cols.push(cn.clone());
                    let mut found = 0;
                    for v in display_group_cols.iter() {
                        if *v == prop.name() {
                            found += 1;
                        }
                    }
                    display_group_cols.push(prop.name());
                    if found == 0 {
                        rename_groups.push((cn, prop.name()));
                    } else {
                        rename_groups.push((cn, format!("{} {}", prop.name(), found + 1)));
                    }
                }
            }
        }

        group_cols.push(COL_AGG_NAME.to_string());
        rename_groups.push((COL_AGG_NAME.to_string(), "Formula".to_string()));

        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(ReorderColumnsNode::try_new(input, group_cols)?),
        });
        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(RenameColumnsNode::try_new(input, rename_groups)?),
        });

        Ok(input)
    }

    fn build_segment_condition(
        &self,
        condition: &SegmentCondition,
        group_id: usize,
    ) -> Result<SegmentExpr> {
        let expr = match condition {
            SegmentCondition::HasPropertyValue { .. } => unimplemented!(),
            SegmentCondition::HadPropertyValue {
                property,
                operation,
                value,
                time,
            } => {
                let filter = property_expression(
                    &self.ctx,
                    &self.metadata,
                    TABLE_EVENTS,
                    &property,
                    operation,
                    value.to_owned(),
                )?;

                SegmentExpr::Count {
                    filter,
                    ts_col: Column::from_qualified_name(COLUMN_CREATED_AT),
                    partition_col: col(group_col(group_id)).try_into_col()?,
                    time_range: time.into(),
                    op: segment::Operator::GtEq,
                    right: 1,
                    time_window: time.try_window(),
                }
            }
            SegmentCondition::DidEvent {
                event,
                filters,
                aggregate,
            } => {
                // event expression
                let mut event_expr = event_expression(&self.ctx, &self.metadata, event)?;
                // apply event filters
                if let Some(filters) = &filters {
                    event_expr = and(
                        event_expr.clone(),
                        event_filters_expression(&self.ctx, &self.metadata, filters)?,
                    )
                }
                match aggregate {
                    DidEventAggregate::Count {
                        operation,
                        value,
                        time,
                    } => SegmentExpr::Count {
                        filter: event_expr,
                        ts_col: Column::from_qualified_name(COLUMN_CREATED_AT),
                        partition_col: col(group_col(group_id)).try_into_col()?,
                        time_range: time.into(),
                        op: operation.into(),
                        right: *value,
                        time_window: time.try_window(),
                    },
                    DidEventAggregate::RelativeCount { .. } => unimplemented!(),
                    DidEventAggregate::AggregateProperty {
                        property,
                        aggregate,
                        operation,
                        value,
                        time,
                    } => SegmentExpr::Aggregate {
                        filter: event_expr,
                        predicate: property_col(&self.ctx, &self.metadata, property)?
                            .try_into_col()?,
                        ts_col: Column::from_qualified_name(COLUMN_CREATED_AT),
                        partition_col: col(group_col(group_id)).try_into_col()?,
                        time_range: time.into(),
                        agg: aggregate.into(),
                        op: operation.into(),
                        right: value.to_owned().unwrap(),
                        time_window: time.try_window(),
                    },
                    DidEventAggregate::HistoricalCount { .. } => {
                        unimplemented!()
                    }
                }
            }
        };

        Ok(expr)
    }

    fn decode_filter_dictionaries(
        &self,
        input: LogicalPlan,
        cols_hash: &mut HashMap<String, ()>,
    ) -> Result<LogicalPlan> {
        let mut decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)> = Vec::new();

        for event in &self.es.events {
            if let Some(filters) = &event.filters {
                for filter in filters {
                    decode_filter_single_dictionary(
                        &self.ctx,
                        &self.metadata,
                        cols_hash,
                        &mut decode_cols,
                        filter,
                    )?;
                }
            }
        }

        if let Some(filters) = &self.es.filters {
            for filter in filters {
                decode_filter_single_dictionary(
                    &self.ctx,
                    &self.metadata,
                    cols_hash,
                    &mut decode_cols,
                    filter,
                )?;
            }
        }

        if decode_cols.is_empty() {
            return Ok(input);
        }

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(DictionaryDecodeNode::try_new(input, decode_cols.clone())?),
        }))
    }

    fn decode_breakdowns_dictionaries(
        &self,
        input: LogicalPlan,
        tbl: &str,
        cols_hash: &mut HashMap<String, ()>,
    ) -> Result<LogicalPlan> {
        let mut decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)> = Vec::new();
        for event in &self.es.events {
            if let Some(breakdowns) = &event.breakdowns {
                breakdowns_to_dicts!(self.metadata, self.ctx,breakdowns, cols_hash, decode_cols);
            }
        }

        if let Some(breakdowns) = &self.es.breakdowns {
            breakdowns_to_dicts!(self.metadata, self.ctx,breakdowns, cols_hash, decode_cols);
        }

        if decode_cols.is_empty() {
            return Ok(input);
        }

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(DictionaryDecodeNode::try_new(input, decode_cols.clone())?),
        }))
    }

    fn build_event_logical_plan(
        &self,
        input: LogicalPlan,
        event_id: usize,
        group_id: usize,
        segment_inputs: Option<Vec<LogicalPlan>>,
    ) -> Result<LogicalPlan> {
        let input =
            self.build_filter_logical_plan(input.clone(), &self.es.events[event_id], group_id)?;

        let (mut input, group_expr) = self.build_aggregate_logical_plan(
            input,
            &self.es.events[event_id],
            event_id,
            group_id,
            segment_inputs,
        )?;
        // unpivot aggregate values into value column
        if self.ctx.format != Format::Compact {
            input = {
                let agg_cols = self.es.events[event_id]
                    .queries
                    .iter()
                    .enumerate()
                    .map(|(idx, q)| match q.agg {
                        Query::CountEvents => format!("{event_id}_{idx}_count"),
                        Query::CountUniqueGroups => format!("{event_id}_{idx}_partitioned_count"),
                        Query::DailyActiveGroups => format!("{event_id}_{idx}_partitioned_count"),
                        Query::WeeklyActiveGroups => unimplemented!(),
                        Query::MonthlyActiveGroups => unimplemented!(),
                        Query::CountPerGroup { .. } => {
                            format!("{event_id}_{idx}_partitioned_count")
                        }
                        Query::AggregatePropertyPerGroup { .. } => {
                            format!("{event_id}_{idx}_partitioned_agg")
                        }
                        Query::AggregateProperty { .. } => format!("{event_id}_{idx}_agg"),
                        Query::QueryFormula { .. } => unimplemented!(),
                    })
                    .collect();

                LogicalPlan::Extension(Extension {
                    node: Arc::new(UnpivotNode::try_new(
                        input,
                        agg_cols,
                        COL_AGG_NAME.to_string(),
                        COL_VALUE.to_string(),
                    )?),
                })
            };
        }

        // input = {
        // let sort_expr = group_expr
        // .into_iter()
        // .map(|expr| {
        // Expr::Sort(expr::Sort {
        // expr: Box::new(expr),
        // asc: true,
        // nulls_first: false,
        // })
        // })
        // .collect::<Vec<_>>();
        // let sort = Sort {
        // expr: sort_expr,
        // input: Arc::new(input),
        // fetch: None,
        // };
        //
        // LogicalPlan::Sort(sort)
        // };
        if self.ctx.format != Format::Compact {
            input = {
                let (from_time, to_time) = self.es.time.range(self.ctx.cur_time);
                let result_cols = time_columns(from_time, to_time, &self.es.interval_unit);
                LogicalPlan::Extension(Extension {
                    node: Arc::new(PivotNode::try_new(
                        input,
                        Column::from_name(COLUMN_CREATED_AT),
                        Column::from_name(COL_VALUE),
                        result_cols,
                    )?),
                })
            };

            let mut rename_rows = vec![];
            for (event_id, event) in self.es.events.iter().enumerate() {
                for (query_id, query) in event.queries.iter().enumerate() {
                    let from = format!("{event_id}_{query_id}_{}", query.agg.initial_name());
                    let to = query.agg.name();
                    rename_rows.push((from, to));
                }
            }

            input = LogicalPlan::Extension(Extension {
                node: Arc::new(RenameColumnRowsNode::try_new(
                    input,
                    Column::new_unqualified(COL_AGG_NAME.to_string()),
                    rename_rows,
                )?),
            });
        }

        input = LogicalPlan::Extension(Extension {
            node: Arc::new(AggregateAndSortColumnsNode::try_new(
                input,
                group_expr.len() - 1 + 2,
            )?), // +2 is segment and agg_name cols that are groups as well
        });

        input = LogicalPlan::Extension(Extension {
            node: Arc::new(LimitGroupsNode::try_new(
                input,
                1,
                group_expr.len() - 1,
                50,
            )?),
        });
        Ok(input)
    }

    /// builds filter plan
    fn build_filter_logical_plan(
        &self,
        input: LogicalPlan,
        event: &Event,
        group_id: usize,
    ) -> Result<LogicalPlan> {
        let cur_time = self.ctx.cur_time;
        // todo add project_id filtering
        let mut expr = binary_expr(
            col(COLUMN_PROJECT_ID),
            Operator::Eq,
            lit(ScalarValue::from(self.ctx.project_id as i64)),
        );
        expr = and(
            expr,
            time_expression(COLUMN_CREATED_AT, input.schema(), &self.es.time, cur_time)?,
        );

        // event expression
        expr = and(
            expr,
            event_expression(&self.ctx, &self.metadata, &event.event)?,
        );
        // apply event filters
        if let Some(filters) = &event.filters {
            expr = and(
                expr.clone(),
                event_filters_expression(&self.ctx, &self.metadata, filters)?,
            )
        }

        // global event filters
        if let Some(filters) = &self.es.filters {
            expr = and(
                expr.clone(),
                event_filters_expression(&self.ctx, &self.metadata, filters)?,
            );
        }

        // global filter
        Ok(LogicalPlan::Filter(Filter::try_new(expr, Arc::new(input))?))
    }

    // builds logical plan for aggregate
    fn build_aggregate_logical_plan(
        &self,
        input: LogicalPlan,
        event: &Event,
        event_id: usize,
        group_id: usize,
        segment_inputs: Option<Vec<LogicalPlan>>,
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
        let mut group_expr: Vec<(Expr, String)> = vec![];

        let ts_col = Expr::Column(Column::from_qualified_name(COLUMN_CREATED_AT));
        let expr_fn = ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(DateTruncFunc::new())),
            args: vec![lit(self.es.interval_unit.as_str()), ts_col],
        };
        let time_expr = Expr::ScalarFunction(expr_fn);

        group_expr.push((
            Expr::Alias(Alias {
                expr: Box::new(time_expr),
                relation: None,
                name: COLUMN_CREATED_AT.to_string(),
            }),
            "created_at".to_string(),
        ));

        // event groups
        if let Some(breakdowns) = &event.breakdowns {
            for breakdown in breakdowns.iter() {
                let prop = match breakdown {
                    Breakdown::Property(p) => match p {
                        PropertyRef::Group(p, group) => self.metadata.group_properties[*group]
                            .get_by_name(self.ctx.project_id, p)?,
                        PropertyRef::Event(p) => self
                            .metadata
                            .event_properties
                            .get_by_name(self.ctx.project_id, p)?,
                        _ => {
                            return Err(QueryError::Unimplemented(
                                "invalid property type".to_string(),
                            ));
                        }
                    },
                };
                group_expr.push((
                    breakdown_expr(&self.ctx, &self.metadata, breakdown)?,
                    prop.column_name(),
                ));
            }
        }

        // common groups
        if let Some(breakdowns) = &self.es.breakdowns {
            for breakdown in breakdowns.iter() {
                let prop = match breakdown {
                    Breakdown::Property(p) => match p {
                        PropertyRef::Group(p, group) => self.metadata.group_properties[*group]
                            .get_by_name(self.ctx.project_id, p)?,
                        PropertyRef::Event(p) => self
                            .metadata
                            .event_properties
                            .get_by_name(self.ctx.project_id, p)?,
                        _ => {
                            return Err(QueryError::Unimplemented(
                                "invalid property type".to_string(),
                            ));
                        }
                    },
                };
                group_expr.push((
                    breakdown_expr(&self.ctx, &self.metadata, breakdown)?,
                    prop.column_name(),
                ));
            }
        }

        let group_expr = input
            .schema()
            .fields()
            .iter()
            .filter_map(|f| {
                group_expr
                    .iter()
                    .find(|v| v.1 == *f.name())
                    .map(|(expr, _)| {
                        (expr.clone(), SortField {
                            data_type: expr.get_type(input.schema()).unwrap(),
                        })
                    })
            })
            .collect::<Vec<_>>();

        let mut agg_expr = Vec::new();
        for (idx, query) in event.queries.iter().enumerate() {
            let agg = match &query.agg {
                Query::CountEvents => AggregateExpr::Count {
                    filter: None,
                    groups: Some(group_expr.clone()),
                    predicate: col(COLUMN_EVENT).try_into_col()?,
                    partition_col: col(group_col(group_id)).try_into_col()?,
                    distinct: false,
                },
                Query::CountUniqueGroups | Query::DailyActiveGroups => {
                    AggregateExpr::PartitionedCount {
                        filter: None,
                        outer_fn: partitioned_aggregate::AggregateFunction::Count,
                        groups: Some(group_expr.clone()),
                        partition_col: col(group_col(group_id)).try_into_col()?,
                        distinct: true,
                    }
                }
                Query::WeeklyActiveGroups => unimplemented!(),
                Query::MonthlyActiveGroups => unimplemented!(),
                Query::CountPerGroup { aggregate } => AggregateExpr::PartitionedCount {
                    filter: None,
                    outer_fn: aggregate.into(),
                    groups: Some(group_expr.clone()),
                    partition_col: col(group_col(group_id)).try_into_col()?,
                    distinct: false,
                },
                Query::AggregatePropertyPerGroup {
                    property,
                    aggregate_per_group,
                    aggregate,
                } => AggregateExpr::PartitionedAggregate {
                    filter: None,
                    inner_fn: aggregate_per_group.into(),
                    outer_fn: aggregate.into(),
                    predicate: property_col(&self.ctx, &self.metadata, property)?.try_into_col()?,
                    groups: Some(group_expr.clone()),
                    partition_col: col(group_col(group_id)).try_into_col()?,
                },
                Query::AggregateProperty {
                    property,
                    aggregate,
                } => AggregateExpr::Aggregate {
                    filter: None,
                    groups: Some(group_expr.clone()),
                    partition_col: col(group_col(group_id)).try_into_col()?,
                    predicate: property_col(&self.ctx, &self.metadata, property)?.try_into_col()?,
                    agg: aggregate.into(),
                },
                Query::QueryFormula { .. } => unimplemented!(),
            };

            agg_expr.push((agg, format!("{event_id}_{idx}")));
        }

        let agg = PartitionedAggregatePartialNode::try_new(
            input,
            segment_inputs.clone(),
            Column::from_qualified_name(group_col(group_id)),
            agg_expr.clone(),
        )?;

        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(agg),
        });

        let final_agg_expr = agg_expr
            .into_iter()
            .map(|(expr, name)| {
                let predicate =
                    Column::from_qualified_name(format!("{}_{}", name, expr.field_names()[0]));
                (
                    expr.change_predicate(Column::from_qualified_name(COLUMN_SEGMENT), predicate),
                    name,
                )
            })
            .collect::<Vec<_>>();
        let agg = PartitionedAggregateFinalNode::try_new(input, final_agg_expr)?;
        let input = LogicalPlan::Extension(Extension {
            node: Arc::new(agg),
        });

        Ok((
            input,
            group_expr.into_iter().map(|(a, _)| a).collect::<Vec<_>>(),
        ))
    }
}

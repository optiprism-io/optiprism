use std::collections::HashMap;
use std::sync::Arc;

use common::query::event_segmentation::Event;
use common::query::event_segmentation::EventSegmentation;
use common::query::event_segmentation::Query;
use common::query::time_columns;
use common::query::Breakdown;
use common::query::DidEventAggregate;
use common::query::PropertyRef;
use common::query::SegmentCondition;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_PROJECT_ID;
use common::types::COLUMN_SEGMENT;
use common::types::COLUMN_USER_ID;
use datafusion_common::Column;
use datafusion_common::ScalarValue;
use datafusion_expr::binary_expr;
use datafusion_expr::col;
use datafusion_expr::expr;
use datafusion_expr::expr::Alias;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_fn::and;
use datafusion_expr::lit;
use datafusion_expr::BuiltinScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::ExprSchemable;
use datafusion_expr::Extension;
use datafusion_expr::Filter;
use datafusion_expr::LogicalPlan;
use datafusion_expr::Operator;
use datafusion_expr::ScalarFunctionDefinition;
use datafusion_expr::Sort;
use metadata::dictionaries::SingleDictionaryProvider;
use metadata::MetadataProvider;

use crate::context::Format;
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
use crate::queries::decode_filter_single_dictionary;
use crate::Context;

pub const COL_AGG_NAME: &str = "agg_name";
const COL_VALUE: &str = "value";

pub struct LogicalPlanBuilder {
    ctx: Context,
    metadata: Arc<MetadataProvider>,
    es: EventSegmentation,
}

macro_rules! breakdowns_to_dicts {
    ($self:expr, $breakdowns:expr, $cols_hash:expr,$decode_cols:expr) => {{
        for breakdown in $breakdowns.iter() {
            match &breakdown {
                Breakdown::Property(prop) => {
                    let p = match prop {
                        PropertyRef::System(name) => $self
                            .metadata
                            .system_properties
                            .get_by_name($self.ctx.project_id, name.as_str())?,
                        PropertyRef::User(name) => $self
                            .metadata
                            .user_properties
                            .get_by_name($self.ctx.project_id, name.as_str())?,
                        PropertyRef::Event(name) => $self
                            .metadata
                            .event_properties
                            .get_by_name($self.ctx.project_id, name.as_str())?,
                        _ => unimplemented!(),
                    };
                    if !p.is_dictionary {
                        continue;
                    }
                    if $cols_hash.contains_key(p.column_name().as_str()) {
                        continue;
                    }

                    let dict = SingleDictionaryProvider::new(
                        $self.ctx.project_id,
                        p.column_name(),
                        $self.metadata.dictionaries.clone(),
                    );
                    let col = Column::from_name(p.column_name());

                    $decode_cols.push((col, Arc::new(dict)));
                    $cols_hash.insert(p.column_name(), ());
                }
            };
        }
    }};
}

impl LogicalPlanBuilder {
    /// creates logical plan for event segmentation
    pub fn build(
        ctx: Context,
        metadata: Arc<MetadataProvider>,
        input: LogicalPlan,
        es: EventSegmentation,
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
                        let expr = builder.build_segment_condition(&condition)?;
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
                    Column::from_qualified_name(COLUMN_USER_ID),
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
                let input = builder.build_event_logical_plan(input.clone(), 0, segment_inputs)?;

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

        input = builder.decode_breakdowns_dictionaries(input, &mut cols_hash)?;

        if ctx.format == Format::Compact {
            return Ok(input);
        }
        let mut group_cols = if ctx.format != Format::Compact {
            vec![COLUMN_EVENT.to_string(), COLUMN_SEGMENT.to_string()]
        } else {
            vec![COLUMN_SEGMENT.to_string()]
        };

        let mut rename_groups = if ctx.format != Format::Compact {
            vec![
                (COLUMN_EVENT.to_string(), "Event".to_string()),
                (COLUMN_SEGMENT.to_string(), "Segment".to_string()),
            ]
        } else {
            vec![(COLUMN_SEGMENT.to_string(), "Segment".to_string())]
        };

        if let Some(breakdowns) = &es.breakdowns {
            for breakdown in breakdowns.iter() {
                let prop = match breakdown {
                    Breakdown::Property(p) => match p {
                        PropertyRef::System(p) => {
                            metadata.system_properties.get_by_name(ctx.project_id, p)?
                        }
                        PropertyRef::User(p) => {
                            metadata.user_properties.get_by_name(ctx.project_id, p)?
                        }
                        PropertyRef::Event(p) => {
                            metadata.event_properties.get_by_name(ctx.project_id, p)?
                        }
                        PropertyRef::Custom(_) => unimplemented!(),
                    },
                };
                let cn = prop.column_name();
                if !group_cols.contains(&cn) {
                    group_cols.push(cn.clone());
                    rename_groups.push((cn, prop.name()));
                }
            }
        }

        for event in &es.events {
            if let Some(breakdowns) = &event.breakdowns {
                for breakdown in breakdowns.iter() {
                    let prop = match breakdown {
                        Breakdown::Property(p) => match p {
                            PropertyRef::System(p) => {
                                metadata.system_properties.get_by_name(ctx.project_id, p)?
                            }
                            PropertyRef::User(p) => {
                                metadata.user_properties.get_by_name(ctx.project_id, p)?
                            }
                            PropertyRef::Event(p) => {
                                metadata.event_properties.get_by_name(ctx.project_id, p)?
                            }
                            PropertyRef::Custom(_) => unimplemented!(),
                        },
                    };
                    let cn = prop.column_name();
                    if !group_cols.contains(&cn) {
                        group_cols.push(cn.clone());
                        rename_groups.push((cn, prop.name()));
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

    fn build_segment_condition(&self, condition: &SegmentCondition) -> Result<SegmentExpr> {
        let expr = match condition {
            SegmentCondition::HasPropertyValue { .. } => unimplemented!(),
            SegmentCondition::HadPropertyValue {
                property_name,
                operation,
                value,
                time,
            } => {
                let property = PropertyRef::User(property_name.to_owned());
                let filter = property_expression(
                    &self.ctx,
                    &self.metadata,
                    &property,
                    operation,
                    value.to_owned(),
                )?;

                SegmentExpr::Count {
                    filter,
                    ts_col: Column::from_qualified_name(COLUMN_CREATED_AT),
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
        cols_hash: &mut HashMap<String, ()>,
    ) -> Result<LogicalPlan> {
        let mut decode_cols: Vec<(Column, Arc<SingleDictionaryProvider>)> = Vec::new();
        for event in &self.es.events {
            if let Some(breakdowns) = &event.breakdowns {
                breakdowns_to_dicts!(self, breakdowns, cols_hash, decode_cols);
            }
        }

        if let Some(breakdowns) = &self.es.breakdowns {
            breakdowns_to_dicts!(self, breakdowns, cols_hash, decode_cols);
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
        segment_inputs: Option<Vec<LogicalPlan>>,
    ) -> Result<LogicalPlan> {
        let input = self.build_filter_logical_plan(input.clone(), &self.es.events[event_id])?;

        let (mut input, group_expr) = self.build_aggregate_logical_plan(
            input,
            &self.es.events[event_id],
            event_id,
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
            node: Arc::new(LimitGroupsNode::try_new(input, 1, group_expr.len() - 1, 50)?),
        });
        Ok(input)
    }

    /// builds filter plan
    fn build_filter_logical_plan(&self, input: LogicalPlan, event: &Event) -> Result<LogicalPlan> {
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
        segment_inputs: Option<Vec<LogicalPlan>>,
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
        let mut group_expr: Vec<(Expr, String)> = vec![];

        let ts_col = Expr::Column(Column::from_qualified_name(COLUMN_CREATED_AT));
        let expr_fn = ScalarFunction {
            func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::DateTrunc),
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
                        PropertyRef::System(p) => self
                            .metadata
                            .system_properties
                            .get_by_name(self.ctx.project_id, p)?,
                        PropertyRef::User(p) => self
                            .metadata
                            .user_properties
                            .get_by_name(self.ctx.project_id, p)?,
                        PropertyRef::Event(p) => self
                            .metadata
                            .event_properties
                            .get_by_name(self.ctx.project_id, p)?,
                        PropertyRef::Custom(_) => unimplemented!(),
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
                        PropertyRef::System(p) => self
                            .metadata
                            .system_properties
                            .get_by_name(self.ctx.project_id, p)?,
                        PropertyRef::User(p) => self
                            .metadata
                            .user_properties
                            .get_by_name(self.ctx.project_id, p)?,
                        PropertyRef::Event(p) => self
                            .metadata
                            .event_properties
                            .get_by_name(self.ctx.project_id, p)?,
                        PropertyRef::Custom(_) => unimplemented!(),
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
                    partition_col: col(COLUMN_USER_ID).try_into_col()?,
                    distinct: false,
                },
                Query::CountUniqueGroups | Query::DailyActiveGroups => {
                    AggregateExpr::PartitionedCount {
                        filter: None,
                        outer_fn: partitioned_aggregate::AggregateFunction::Count,
                        groups: Some(group_expr.clone()),
                        partition_col: col(COLUMN_USER_ID).try_into_col()?,
                        distinct: true,
                    }
                }
                Query::WeeklyActiveGroups => unimplemented!(),
                Query::MonthlyActiveGroups => unimplemented!(),
                Query::CountPerGroup { aggregate } => AggregateExpr::PartitionedCount {
                    filter: None,
                    outer_fn: aggregate.into(),
                    groups: Some(group_expr.clone()),
                    partition_col: col(COLUMN_USER_ID).try_into_col()?,
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
                    partition_col: col(COLUMN_USER_ID).try_into_col()?,
                },
                Query::AggregateProperty {
                    property,
                    aggregate,
                } => AggregateExpr::Aggregate {
                    filter: None,
                    groups: Some(group_expr.clone()),
                    partition_col: col(COLUMN_USER_ID).try_into_col()?,
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
            Column::from_qualified_name(COLUMN_USER_ID),
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

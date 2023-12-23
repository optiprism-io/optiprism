use std::collections::HashMap;
use std::sync::Arc;

use common::query::event_segmentation::Breakdown;
use common::query::event_segmentation::DidEventAggregate;
use common::query::event_segmentation::Event;
use common::query::event_segmentation::EventSegmentation;
use common::query::event_segmentation::Query;
use common::query::event_segmentation::SegmentCondition;
use common::query::time_columns;
use common::query::EventFilter;
use common::query::PropertyRef;
use datafusion_common::Column;
use datafusion_expr::col;
use datafusion_expr::expr;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_fn::and;
use datafusion_expr::lit;
use datafusion_expr::BuiltinScalarFunction;
use datafusion_expr::Expr;
use datafusion_expr::ExprSchemable;
use datafusion_expr::Extension;
use datafusion_expr::Filter;
use datafusion_expr::LogicalPlan;
use datafusion_expr::Sort;
use futures::executor;
use metadata::dictionaries::provider_impl::SingleDictionaryProvider;
use metadata::MetadataProvider;
use tracing::debug;

use crate::context::Format;
use crate::error::Result;
use crate::event_fields;
use crate::expr::event_expression;
use crate::expr::property_col;
use crate::expr::property_expression;
use crate::expr::time_expression;
use crate::logical_plan::dictionary_decode::DictionaryDecodeNode;
use crate::logical_plan::expr::multi_and;
use crate::logical_plan::merge::MergeNode;
use crate::logical_plan::partitioned_aggregate;
use crate::logical_plan::partitioned_aggregate::AggregateExpr;
use crate::logical_plan::partitioned_aggregate::PartitionedAggregateNode;
use crate::logical_plan::partitioned_aggregate::SortField;
use crate::logical_plan::pivot::PivotNode;
use crate::logical_plan::segment;
use crate::logical_plan::segment::SegmentExpr;
use crate::logical_plan::segment::SegmentNode;
use crate::logical_plan::unpivot::UnpivotNode;
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
                    if $cols_hash.contains_key(prop) {
                        continue;
                    }
                    $cols_hash.insert(prop.to_owned(), ());

                    match prop {
                        PropertyRef::System(name) => {
                            dictionary_prop_to_col!($self, system_properties, name, $decode_cols)
                        }
                        PropertyRef::User(name) => {
                            dictionary_prop_to_col!($self, user_properties, name, $decode_cols)
                        }
                        PropertyRef::Event(name) => {
                            dictionary_prop_to_col!($self, event_properties, name, $decode_cols)
                        }
                        _ => {}
                    }
                }
            }
        }
    }};
}

macro_rules! dictionary_prop_to_col {
    ($self:expr, $md_namespace:ident, $prop_name:expr,  $decode_cols:expr) => {{
        let prop = $self.metadata.$md_namespace.get_by_name(
            $self.ctx.organization_id,
            $self.ctx.project_id,
            $prop_name.as_str(),
        )?;
        if !prop.is_dictionary {
            continue;
        }

        let col_name = prop.column_name();
        let dict = SingleDictionaryProvider::new(
            $self.ctx.organization_id,
            $self.ctx.project_id,
            col_name.clone(),
            $self.metadata.dictionaries.clone(),
        );
        let col = Column::from_name(col_name);

        $decode_cols.push((col, Arc::new(dict)));
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
        debug!("{:?}", es);
        let events = es.events.clone();
        let builder = LogicalPlanBuilder {
            ctx: ctx.clone(),
            metadata,
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
                    Column::from_qualified_name(event_fields::USER_ID),
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

        // build main query
        let mut input = match events.len() {
            1 => builder.build_event_logical_plan(input.clone(), 0, segment_inputs)?,
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

                // merge multiple results into one schema
                LogicalPlan::Extension(Extension {
                    node: Arc::new(MergeNode::try_new(inputs)?),
                })
            }
        };

        input = builder.decode_dictionaries(input)?;

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
                    ts_col: Column::from_qualified_name(event_fields::CREATED_AT),
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
                    event_expr = and(event_expr.clone(), self.event_filters_expression(filters)?)
                }
                match aggregate {
                    DidEventAggregate::Count {
                        operation,
                        value,
                        time,
                    } => SegmentExpr::Count {
                        filter: event_expr,
                        ts_col: Column::from_qualified_name(event_fields::CREATED_AT),
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
                        ts_col: Column::from_qualified_name(event_fields::CREATED_AT),
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

    fn decode_dictionaries(&self, input: LogicalPlan) -> Result<LogicalPlan> {
        let mut cols_hash: HashMap<PropertyRef, ()> = HashMap::new();
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
            node: Arc::new(DictionaryDecodeNode::try_new(input, decode_cols)?),
        }))
    }

    fn build_event_logical_plan(
        &self,
        input: LogicalPlan,
        event_id: usize,
        segment_inputs: Option<Vec<LogicalPlan>>,
    ) -> Result<LogicalPlan> {
        let input = self.build_filter_logical_plan(input.clone(), &self.es.events[event_id])?;
        let (mut input, group_expr) =
            self.build_aggregate_logical_plan(input, &self.es.events[event_id], segment_inputs)?;

        // unpivot aggregate values into value column
        if self.ctx.format != Format::Compact {
            input = {
                let agg_cols = self.es.events[event_id]
                    .queries
                    .iter()
                    .enumerate()
                    .map(|(idx, q)| match q.agg {
                        Query::CountEvents => format!("{idx}_count"),
                        Query::CountUniqueGroups => format!("{idx}_partitioned_count"),
                        Query::DailyActiveGroups => format!("{idx}_partitioned_count"),
                        Query::WeeklyActiveGroups => unimplemented!(),
                        Query::MonthlyActiveGroups => unimplemented!(),
                        Query::CountPerGroup { .. } => format!("{idx}_partitioned_count"),
                        Query::AggregatePropertyPerGroup { .. } => format!("{idx}_partitioned_agg"),
                        Query::AggregateProperty { .. } => format!("{idx}_agg"),
                        Query::QueryFormula { .. } => format!("{idx}_count"),
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

        input = {
            let sort_expr = group_expr
                .into_iter()
                .map(|expr| {
                    Expr::Sort(expr::Sort {
                        expr: Box::new(expr),
                        asc: true,
                        nulls_first: false,
                    })
                })
                .collect::<Vec<_>>();
            let sort = Sort {
                expr: sort_expr,
                input: Arc::new(input),
                fetch: None,
            };

            LogicalPlan::Sort(sort)
        };

        if self.ctx.format != Format::Compact {
            // pivot date
            input = {
                let (from_time, to_time) = self.es.time.range(self.ctx.cur_time);
                let result_cols = time_columns(from_time, to_time, &self.es.interval_unit);
                LogicalPlan::Extension(Extension {
                    node: Arc::new(PivotNode::try_new(
                        input,
                        Column::from_name(event_fields::CREATED_AT),
                        Column::from_name(COL_VALUE),
                        result_cols,
                    )?),
                })
            };
        }
        Ok(input)
    }

    /// builds filter plan
    fn build_filter_logical_plan(&self, input: LogicalPlan, event: &Event) -> Result<LogicalPlan> {
        let cur_time = self.ctx.cur_time;
        // let cur_time = match &self.es.interval_unit {
        // TimeIntervalUnit::Second => self
        // .ctx
        // .cur_time
        // .duration_trunc(Duration::seconds(1))
        // .unwrap(),
        // TimeIntervalUnit::Minute => self
        // .ctx
        // .cur_time
        // .duration_trunc(Duration::minutes(1))
        // .unwrap(),
        // TimeIntervalUnit::Hour => self
        // .ctx
        // .cur_time
        // .duration_trunc(Duration::hours(1))
        // .unwrap(),
        // TimeIntervalUnit::Day => self.ctx.cur_time.duration_trunc(Duration::days(1)).unwrap(),
        // TimeIntervalUnit::Week => self.ctx.cur_time.beginning_of_week(),
        // TimeIntervalUnit::Month => self.ctx.cur_time.beginning_of_month(),
        // TimeIntervalUnit::Year => self.ctx.cur_time.beginning_of_year(),
        // };
        // let cur_time = self.cur_time.duration_trunc(trunc).unwrap();
        // time expression

        // todo add project_id filtering
        let mut expr = time_expression(
            event_fields::CREATED_AT,
            input.schema(),
            &self.es.time,
            cur_time,
        )?;

        // event expression
        expr = and(
            expr,
            event_expression(&self.ctx, &self.metadata, &event.event)?,
        );
        // apply event filters
        if let Some(filters) = &event.filters {
            expr = and(expr.clone(), self.event_filters_expression(filters)?)
        }

        // global event filters
        if let Some(filters) = &self.es.filters {
            expr = and(expr.clone(), self.event_filters_expression(filters)?);
        }

        // global filter
        Ok(LogicalPlan::Filter(Filter::try_new(expr, Arc::new(input))?))
    }

    // builds logical plan for aggregate
    fn build_aggregate_logical_plan(
        &self,
        input: LogicalPlan,
        event: &Event,
        segment_inputs: Option<Vec<LogicalPlan>>,
    ) -> Result<(LogicalPlan, Vec<Expr>)> {
        let mut group_expr: Vec<Expr> = vec![];

        let ts_col = Expr::Column(Column::from_qualified_name(event_fields::CREATED_AT));
        let expr_fn = ScalarFunction {
            fun: BuiltinScalarFunction::DateTrunc,
            args: vec![lit(self.es.interval_unit.as_str()), ts_col],
        };
        let time_expr = Expr::ScalarFunction(expr_fn);

        // group_expr.push(Expr::Alias(
        // Box::new(lit(event.event.name())),
        // event_fields::EVENT.to_string(),
        // ));
        group_expr.push(Expr::Alias(
            Box::new(time_expr),
            event_fields::CREATED_AT.to_string(),
        ));

        // event groups
        if let Some(breakdowns) = &event.breakdowns {
            for breakdown in breakdowns.iter() {
                group_expr.push(self.breakdown_expr(breakdown)?);
            }
        }

        // common groups
        if let Some(breakdowns) = &self.es.breakdowns {
            for breakdown in breakdowns.iter() {
                group_expr.push(self.breakdown_expr(breakdown)?);
            }
        }

        let group_expr = group_expr
            .iter()
            .enumerate()
            .map(|(_idx, expr)| {
                (expr.to_owned(), SortField {
                    data_type: expr.get_type(input.schema()).unwrap(),
                })
            })
            .collect::<Vec<_>>();
        let mut aggr_expr = Vec::new();

        for (idx, query) in event.queries.iter().enumerate() {
            let agg = match &query.agg {
                Query::CountEvents => AggregateExpr::Count {
                    filter: None,
                    groups: Some(group_expr.clone()),
                    predicate: col(event_fields::EVENT).try_into_col()?,
                    partition_col: col(event_fields::USER_ID).try_into_col()?,
                    distinct: false,
                },
                Query::CountUniqueGroups | Query::DailyActiveGroups => {
                    AggregateExpr::PartitionedCount {
                        filter: None,
                        outer_fn: partitioned_aggregate::AggregateFunction::Count,
                        groups: Some(group_expr.clone()),
                        partition_col: col(event_fields::USER_ID).try_into_col()?,
                        distinct: true,
                    }
                }
                Query::WeeklyActiveGroups => unimplemented!(),
                Query::MonthlyActiveGroups => unimplemented!(),
                Query::CountPerGroup { aggregate } => AggregateExpr::PartitionedCount {
                    filter: None,
                    outer_fn: aggregate.into(),
                    groups: Some(group_expr.clone()),
                    partition_col: col(event_fields::USER_ID).try_into_col()?,
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
                    partition_col: col(event_fields::USER_ID).try_into_col()?,
                },
                Query::AggregateProperty {
                    property,
                    aggregate,
                } => AggregateExpr::Aggregate {
                    filter: None,
                    groups: Some(group_expr.clone()),
                    partition_col: col(event_fields::USER_ID).try_into_col()?,
                    predicate: property_col(&self.ctx, &self.metadata, property)?.try_into_col()?,
                    agg: aggregate.into(),
                },
                Query::QueryFormula { .. } => unimplemented!(),
            };

            aggr_expr.push((agg, idx.to_string()));
        }

        // todo check for duplicates

        let agg_node = PartitionedAggregateNode::try_new(
            input,
            segment_inputs,
            Column::from_qualified_name(event_fields::USER_ID),
            aggr_expr,
        )?;

        Ok((
            LogicalPlan::Extension(Extension {
                node: Arc::new(agg_node),
            }),
            group_expr.into_iter().map(|(a, _b)| a).collect::<Vec<_>>(),
        ))
    }

    /// builds event filters expression
    fn event_filters_expression(&self, filters: &[EventFilter]) -> Result<Expr> {
        // iterate over filters
        let filters_exprs = filters
            .iter()
            .map(|filter| {
                // match filter type
                match filter {
                    EventFilter::Property {
                        property,
                        operation,
                        value,
                    } => property_expression(
                        &self.ctx,
                        &self.metadata,
                        property,
                        operation,
                        value.to_owned(),
                    ),
                }
            })
            .collect::<Result<Vec<Expr>>>()?;

        if filters_exprs.len() == 1 {
            return Ok(filters_exprs[0].clone());
        }

        Ok(multi_and(filters_exprs))
    }

    // builds breakdown expression
    fn breakdown_expr(&self, breakdown: &Breakdown) -> Result<Expr> {
        match breakdown {
            Breakdown::Property(prop_ref) => match prop_ref {
                PropertyRef::System(_prop_name)| PropertyRef::User(_prop_name) | PropertyRef::Event(_prop_name) => {
                    let prop_col = property_col(&self.ctx, &self.metadata, prop_ref)?;
                    Ok(prop_col)
                }
                PropertyRef::Custom(_) => unimplemented!(),
            },
        }
    }
}

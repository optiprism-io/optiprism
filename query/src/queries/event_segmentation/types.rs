use crate::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction;
use crate::queries::types::{EventRef, PropValueOperation, PropertyRef, QueryTime, TimeUnit};
use chrono::{DateTime, Utc};
use datafusion_commonValue;
use datafusion_expr::AggregateFunction;

#[derive(Clone, Debug)]
pub enum SegmentTime {
    Between {
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    },
    From(DateTime<Utc>),
    Last {
        n: i64,
        unit: TimeUnit,
    },
    AfterFirstUse {
        within: i64,
        unit: TimeUnit,
    },
    WindowEach {
        unit: TimeUnit,
    },
}

#[derive(Clone, Debug)]
pub enum ChartType {
    Line,
    Bar,
}

#[derive(Clone, Debug)]
pub enum Analysis {
    Linear,
    RollingAverage { window: usize, unit: TimeUnit },
    WindowAverage { window: usize, unit: TimeUnit },
    Cumulative,
}

#[derive(Clone, Debug)]
pub struct Compare {
    pub offset: usize,
    pub unit: TimeUnit,
}

#[derive(Clone, Debug)]
pub enum QueryAggregate {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
    Percentile25th,
    Percentile75th,
    Percentile90th,
    Percentile99th,
}

impl QueryAggregate {
    pub fn aggregate_function(&self) -> AggregateFunction {
        match self {
            QueryAggregate::Min => AggregateFunction::Min,
            QueryAggregate::Max => AggregateFunction::Max,
            QueryAggregate::Sum => AggregateFunction::Sum,
            QueryAggregate::Avg => AggregateFunction::Avg,
            QueryAggregate::Median => unimplemented!(),
            QueryAggregate::DistinctCount => unimplemented!(),
            QueryAggregate::Percentile25th => unimplemented!(),
            QueryAggregate::Percentile75th => unimplemented!(),
            QueryAggregate::Percentile90th => unimplemented!(),
            QueryAggregate::Percentile99th => unimplemented!(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum QueryAggregatePerGroup {
    Min,
    Max,
    Sum,
    Avg,
    Median,
    DistinctCount,
}

#[derive(Clone, Debug)]
pub enum QueryPerGroup {
    CountEvents,
}

#[derive(Clone, Debug)]
pub enum Query {
    CountEvents,
    CountUniqueGroups,
    DailyActiveGroups,
    WeeklyActiveGroups,
    MonthlyActiveGroups,
    CountPerGroup {
        aggregate: AggregateFunction,
    },
    AggregatePropertyPerGroup {
        property: PropertyRef,
        aggregate_per_group: PartitionedAggregateFunction,
        aggregate: AggregateFunction,
    },
    AggregateProperty {
        property: PropertyRef,
        aggregate: AggregateFunction,
    },
    QueryFormula {
        formula: String,
    },
}

#[derive(Clone, Debug)]
pub struct NamedQuery {
    pub agg: Query,
    pub name: Option<String>,
}

impl NamedQuery {
    pub fn new(agg: Query, name: Option<String>) -> Self {
        NamedQuery { name, agg }
    }
}

#[derive(Clone, Debug)]
pub enum EventFilter {
    Property {
        property: PropertyRef,
        operation: PropValueOperation,
        value: Option<Vec<ScalarValue>>,
    },
}

#[derive(Clone, Debug)]
pub enum Breakdown {
    Property(PropertyRef),
}

#[derive(Clone, Debug)]
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<EventFilter>>,
    pub breakdowns: Option<Vec<Breakdown>>,
    pub queries: Vec<NamedQuery>,
}

impl Event {
    pub fn new(
        event: EventRef,
        filters: Option<Vec<EventFilter>>,
        breakdowns: Option<Vec<Breakdown>>,
        queries: Vec<NamedQuery>,
    ) -> Self {
        Event {
            event,
            filters,
            breakdowns,
            queries,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EventSegmentation {
    pub time: QueryTime,
    pub group: String,
    pub interval_unit: TimeUnit,
    pub chart_type: ChartType,
    pub analysis: Analysis,
    pub compare: Option<Compare>,
    pub events: Vec<Event>,
    pub filters: Option<Vec<EventFilter>>,
    pub breakdowns: Option<Vec<Breakdown>>,
}

#[cfg(test)]
mod tests {
    use crate::event_fields;
    use crate::physical_plan::expressions::partitioned_aggregate::PartitionedAggregateFunction;
    use crate::queries::event_segmentation::types::{
        Analysis, Breakdown, ChartType, Compare, Event, EventFilter, EventSegmentation, NamedQuery,
        Query,
    };
    use crate::queries::types::{EventRef, PropValueOperation, PropertyRef, QueryTime, TimeUnit};
    use chrono::{DateTime, Utc};
    use datafusion_commonValue;
    use datafusion_expr::AggregateFunction;

    #[test]
    fn test_serialize() {
        let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let _es = EventSegmentation {
            time: QueryTime::Between { from, to },
            group: event_fields::USER_ID.to_string(),
            interval_unit: TimeUnit::Minute,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: Some(Compare {
                offset: 1,
                unit: TimeUnit::Second,
            }),
            events: vec![Event::new(
                EventRef::Regular("e1".to_string()),
                Some(vec![
                    EventFilter::Property {
                        property: PropertyRef::User("p1".to_string()),
                        operation: PropValueOperation::Eq,
                        value: Some(vec![ScalarValue::Boolean(Some(true))]),
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event("p2".to_string()),
                        operation: PropValueOperation::Eq,
                        value: Some(vec![ScalarValue::Boolean(None)]),
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event("p3".to_string()),
                        operation: PropValueOperation::Empty,
                        value: None,
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event("p4".to_string()),
                        operation: PropValueOperation::Eq,
                        value: Some(vec![ScalarValue::Utf8(Some("s".to_string()))]),
                    },
                    EventFilter::Property {
                        property: PropertyRef::Event("p5".to_string()),
                        operation: PropValueOperation::Eq,
                        value: Some(vec![ScalarValue::Utf8(None)]),
                    },
                ]),
                Some(vec![Breakdown::Property(PropertyRef::User(
                    "Device".to_string(),
                ))]),
                vec![
                    NamedQuery::new(Query::CountEvents, Some("count".to_string())),
                    NamedQuery::new(
                        Query::CountUniqueGroups,
                        Some("count_unique_users".to_string()),
                    ),
                    NamedQuery::new(
                        Query::CountPerGroup {
                            aggregate: AggregateFunction::Avg,
                        },
                        Some("count_per_user".to_string()),
                    ),
                    NamedQuery::new(
                        Query::AggregatePropertyPerGroup {
                            property: PropertyRef::Event("Revenue".to_string()),
                            aggregate_per_group: PartitionedAggregateFunction::Sum,
                            aggregate: AggregateFunction::Avg,
                        },
                        Some("avg_revenue_per_user".to_string()),
                    ),
                    NamedQuery::new(
                        Query::AggregateProperty {
                            property: PropertyRef::Event("Revenue".to_string()),
                            aggregate: AggregateFunction::Sum,
                        },
                        Some("sum_revenue".to_string()),
                    ),
                ],
            )],
            filters: Some(vec![EventFilter::Property {
                property: PropertyRef::User("p1".to_string()),
                operation: PropValueOperation::Eq,
                value: Some(vec![ScalarValue::Boolean(Some(true))]),
            }]),
            breakdowns: Some(vec![Breakdown::Property(PropertyRef::User(
                "Country".to_string(),
            ))]),
        };

        /*let j = serde_json::to_string_pretty(&es).unwrap();
        println!("{}", j);*/
    }
}

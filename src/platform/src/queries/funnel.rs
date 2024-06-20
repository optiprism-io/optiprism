use std::collections::HashMap;
use std::sync::Arc;

use common::GROUPS_COUNT;
use metadata::MetadataProvider;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;

use crate::error::Result;
use crate::{Breakdown, EventGroupedFilterGroup, QueryTime, Segment, TimeIntervalUnit, validate_event, validate_event_filter, validate_event_property};
use crate::EventGroupedFilters;
use crate::EventRef;
use crate::PlatformError;
use crate::PropValueFilter;
use crate::PropertyRef;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FunnelRequest {
    pub time: QueryTime,
    pub group: usize,
    pub steps: Vec<Step>,
    pub time_window: TimeWindow,
    pub chart_type: ChartType,
    pub count: Count,
    pub filter: Option<Filter>,
    pub touch: Touch,
    pub attribution: Option<Touch>,
    pub holding_constants: Option<Vec<PropertyRef>>,
    pub exclude: Option<Vec<Exclude>>,
    pub breakdowns: Option<Vec<Breakdown>>,
    pub segments: Option<Vec<Segment>>,
    pub filters: Option<EventGroupedFilters>,
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub events: Vec<Event>,
    pub order: StepOrder,
}

impl Into<common::funnel::Step> for Step {
    fn into(self) -> common::funnel::Step {
        common::funnel::Step {
            events: self
                .events
                .iter()
                .map(|e| e.to_owned().into())
                .collect::<Vec<_>>(),
            order: self.order.into(),
        }
    }
}

impl Into<Step> for common::funnel::Step {
    fn into(self) -> Step {
        Step {
            events: self
                .events
                .iter()
                .map(|e| e.to_owned().into())
                .collect::<Vec<_>>(),
            order: self.order.into(),
        }
    }
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Order {
    Any,
    Exact,
}

impl Into<common::funnel::Order> for Order {
    fn into(self) -> common::funnel::Order {
        match self {
            Order::Any => common::funnel::Order::Any,
            Order::Exact => common::funnel::Order::Exact,
        }
    }
}

impl Into<Order> for common::funnel::Order {
    fn into(self) -> Order {
        match self {
            common::funnel::Order::Any => Order::Any,
            common::funnel::Order::Exact => Order::Exact,
        }
    }
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Event {
    #[serde(flatten)]
    pub event: EventRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<PropValueFilter>>,
}

impl Into<common::funnel::Event> for Event {
    fn into(self) -> common::funnel::Event {
        common::funnel::Event {
            event: self.event.into(),
            filters: self
                .filters
                .map(|f| f.iter().map(|e| e.to_owned().into()).collect::<Vec<_>>()),
        }
    }
}

impl Into<Event> for common::funnel::Event {
    fn into(self) -> Event {
        Event {
            event: self.event.into(),
            filters: self
                .filters
                .map(|f| f.iter().map(|e| e.to_owned().into()).collect::<Vec<_>>()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TimeWindow {
    pub n: usize,
    pub unit: TimeIntervalUnitSession,
}

impl Into<common::funnel::TimeWindow> for TimeWindow {
    fn into(self) -> common::funnel::TimeWindow {
        common::funnel::TimeWindow {
            n: self.n,
            unit: self.unit.into(),
        }
    }
}

impl Into<TimeWindow> for common::funnel::TimeWindow {
    fn into(self) -> TimeWindow {
        TimeWindow {
            n: self.n,
            unit: self.unit.into(),
        }
    }
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TimeIntervalUnitSession {
    Hour,
    Day,
    Week,
    Month,
    Year,
    Session,
}

impl Into<common::funnel::TimeIntervalUnitSession> for TimeIntervalUnitSession {
    fn into(self) -> common::funnel::TimeIntervalUnitSession {
        match self {
            TimeIntervalUnitSession::Hour => common::funnel::TimeIntervalUnitSession::Hour,
            TimeIntervalUnitSession::Day => common::funnel::TimeIntervalUnitSession::Day,
            TimeIntervalUnitSession::Week => common::funnel::TimeIntervalUnitSession::Week,
            TimeIntervalUnitSession::Month => common::funnel::TimeIntervalUnitSession::Month,
            TimeIntervalUnitSession::Year => common::funnel::TimeIntervalUnitSession::Year,
            TimeIntervalUnitSession::Session => {
                common::funnel::TimeIntervalUnitSession::Session
            }
        }
    }
}

impl Into<TimeIntervalUnitSession> for common::funnel::TimeIntervalUnitSession {
    fn into(self) -> TimeIntervalUnitSession {
        match self {
            common::funnel::TimeIntervalUnitSession::Hour => TimeIntervalUnitSession::Hour,
            common::funnel::TimeIntervalUnitSession::Day => TimeIntervalUnitSession::Day,
            common::funnel::TimeIntervalUnitSession::Week => TimeIntervalUnitSession::Week,
            common::funnel::TimeIntervalUnitSession::Month => TimeIntervalUnitSession::Month,
            common::funnel::TimeIntervalUnitSession::Year => TimeIntervalUnitSession::Year,
            common::funnel::TimeIntervalUnitSession::Session => {
                TimeIntervalUnitSession::Session
            }
        }
    }
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum StepOrder {
    Exact,
    Any { steps: Vec<(usize, usize)> }, // any of the steps
}

impl Into<common::funnel::StepOrder> for StepOrder {
    fn into(self) -> common::funnel::StepOrder {
        match self {
            StepOrder::Exact => common::funnel::StepOrder::Exact,
            StepOrder::Any { steps } => common::funnel::StepOrder::Any(steps),
        }
    }
}

impl Into<StepOrder> for common::funnel::StepOrder {
    fn into(self) -> StepOrder {
        match self {
            common::funnel::StepOrder::Exact => StepOrder::Exact,
            common::funnel::StepOrder::Any(steps) => StepOrder::Any { steps },
        }
    }
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ExcludeSteps {
    All,
    Between { from: usize, to: usize },
}

impl Into<common::funnel::ExcludeSteps> for ExcludeSteps {
    fn into(self) -> common::funnel::ExcludeSteps {
        match self {
            ExcludeSteps::All => common::funnel::ExcludeSteps::All,
            ExcludeSteps::Between { from, to } => {
                common::funnel::ExcludeSteps::Between(from, to)
            }
        }
    }
}

impl Into<ExcludeSteps> for common::funnel::ExcludeSteps {
    fn into(self) -> ExcludeSteps {
        match self {
            common::funnel::ExcludeSteps::All => ExcludeSteps::All,
            common::funnel::ExcludeSteps::Between(from, to) => {
                ExcludeSteps::Between { from, to }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Count {
    Unique,
    NonUnique,
    Session,
}

impl Into<common::funnel::Count> for Count {
    fn into(self) -> common::funnel::Count {
        match self {
            Count::Unique => common::funnel::Count::Unique,
            Count::NonUnique => common::funnel::Count::NonUnique,
            Count::Session => common::funnel::Count::Session,
        }
    }
}

impl Into<Count> for common::funnel::Count {
    fn into(self) -> Count {
        match self {
            common::funnel::Count::Unique => Count::Unique,
            common::funnel::Count::NonUnique => Count::NonUnique,
            common::funnel::Count::Session => Count::Session,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Exclude {
    pub event: Event,
    pub steps: Option<ExcludeSteps>,
}

impl Into<common::funnel::Exclude> for Exclude {
    fn into(self) -> common::funnel::Exclude {
        common::funnel::Exclude {
            event: self.event.into(),
            steps: self.steps.map(|s| s.into()),
        }
    }
}

impl Into<Exclude> for common::funnel::Exclude {
    fn into(self) -> Exclude {
        Exclude {
            event: self.event.into(),
            steps: self.steps.map(|s| s.into()),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Filter {
    // funnel should fail on any step
    DropOffOnAnyStep,
    // funnel should fail on certain step
    DropOffOnStep { step: usize },
    TimeToConvert { from: i64, to: i64 }, // conversion should be within certain window
}

impl Into<common::funnel::Filter> for Filter {
    fn into(self) -> common::funnel::Filter {
        match self {
            Filter::DropOffOnAnyStep => common::funnel::Filter::DropOffOnAnyStep,
            Filter::DropOffOnStep { step } => common::funnel::Filter::DropOffOnStep(step),
            Filter::TimeToConvert { from, to } => {
                common::funnel::Filter::TimeToConvert(from, to)
            }
        }
    }
}

impl Into<Filter> for common::funnel::Filter {
    fn into(self) -> Filter {
        match self {
            common::funnel::Filter::DropOffOnAnyStep => Filter::DropOffOnAnyStep,
            common::funnel::Filter::DropOffOnStep(step) => Filter::DropOffOnStep { step },
            common::funnel::Filter::TimeToConvert(from, to) => {
                Filter::TimeToConvert { from, to }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Touch {
    First,
    Last,
    Step { step: usize },
}

impl Into<common::funnel::Touch> for Touch {
    fn into(self) -> common::funnel::Touch {
        match self {
            Touch::First => common::funnel::Touch::First,
            Touch::Last => common::funnel::Touch::Last,
            Touch::Step { step } => common::funnel::Touch::Step { step },
        }
    }
}

impl Into<Touch> for common::funnel::Touch {
    fn into(self) -> Touch {
        match self {
            common::funnel::Touch::First => Touch::First,
            common::funnel::Touch::Last => Touch::Last,
            common::funnel::Touch::Step { step } => Touch::Step { step },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ChartType {
    Steps,
    #[serde(rename_all = "camelCase")]
    ConversionOverTime {
        interval_unit: TimeIntervalUnit,
    },
    #[serde(rename_all = "camelCase")]
    TimeToConvert {
        interval_unit: TimeIntervalUnit,
        min_interval: i64,
        max_interval: i64,
    },
    Frequency,
}

impl ChartType {
    pub fn time_interval(self) -> Option<TimeIntervalUnit> {
        match self {
            ChartType::Steps => None,
            ChartType::ConversionOverTime { interval_unit } => Some(interval_unit),
            ChartType::TimeToConvert { interval_unit, .. } => Some(interval_unit),
            ChartType::Frequency => None,
        }
    }
}
impl Into<common::funnel::ChartType> for ChartType {
    fn into(self) -> common::funnel::ChartType {
        match self {
            ChartType::Steps => common::funnel::ChartType::Steps,
            ChartType::ConversionOverTime { interval_unit } => {
                common::funnel::ChartType::ConversionOverTime {
                    interval_unit: interval_unit.into(),
                }
            }
            ChartType::TimeToConvert {
                interval_unit,
                min_interval,
                max_interval,
            } => common::funnel::ChartType::TimeToConvert {
                interval_unit: interval_unit.into(),
                min_interval,
                max_interval,
            },
            ChartType::Frequency => common::funnel::ChartType::Frequency,
        }
    }
}

impl Into<ChartType> for common::funnel::ChartType {
    fn into(self) -> ChartType {
        match self {
            common::funnel::ChartType::Steps => ChartType::Steps,
            common::funnel::ChartType::ConversionOverTime { interval_unit } => {
                ChartType::ConversionOverTime {
                    interval_unit: interval_unit.into(),
                }
            }
            common::funnel::ChartType::TimeToConvert {
                interval_unit,
                min_interval,
                max_interval,
            } => ChartType::TimeToConvert {
                interval_unit: interval_unit.into(),
                min_interval,
                max_interval,
            },
            common::funnel::ChartType::Frequency => ChartType::Frequency,
        }
    }
}

impl Into<common::funnel::Funnel> for FunnelRequest {
    fn into(self) -> common::funnel::Funnel {
        common::funnel::Funnel {
            time: self.time.into(),
            group_id: self.group,
            steps: self
                .steps
                .iter()
                .map(|step| step.to_owned().into())
                .collect::<Vec<_>>(),
            time_window: self.time_window.into(),
            chart_type: self.chart_type.into(),
            count: self.count.into(),
            filter: self.filter.map(|f| f.into()),
            touch: self.touch.into(),
            attribution: self.attribution.map(|attr| attr.into()),
            holding_constants: self
                .holding_constants
                .map(|hc| hc.iter().map(|p| p.to_owned().into()).collect::<Vec<_>>()),
            exclude: self
                .exclude
                .map(|ex| ex.iter().map(|e| e.to_owned().into()).collect::<Vec<_>>()),
            breakdowns: self
                .breakdowns
                .map(|b| b.iter().map(|b| b.to_owned().into()).collect::<Vec<_>>()),
            segments: self
                .segments
                .map(|s| s.iter().map(|s| s.to_owned().into()).collect::<Vec<_>>()),
            filters: self.filters.map(|v| {
                v.groups[0]
                    .filters
                    .iter()
                    .map(|f| f.to_owned().into())
                    .collect::<Vec<_>>()
            }),
        }
    }
}

impl Into<FunnelRequest> for common::funnel::Funnel {
    fn into(self) -> FunnelRequest {
        FunnelRequest {
            time: self.time.into(),
            group: self.group_id,
            steps: self
                .steps
                .iter()
                .map(|step| step.to_owned().into())
                .collect::<Vec<_>>(),
            time_window: self.time_window.into(),
            chart_type: self.chart_type.into(),
            count: self.count.into(),
            filter: self.filter.map(|f| f.into()),
            touch: self.touch.into(),
            attribution: self.attribution.map(|attr| attr.into()),
            holding_constants: self
                .holding_constants
                .map(|hc| hc.iter().map(|p| p.to_owned().into()).collect::<Vec<_>>()),
            exclude: self
                .exclude
                .map(|ex| ex.iter().map(|e| e.to_owned().into()).collect::<Vec<_>>()),
            breakdowns: self
                .breakdowns
                .map(|b| b.iter().map(|b| b.to_owned().into()).collect::<Vec<_>>()),
            segments: self
                .segments
                .map(|s| s.iter().map(|s| s.to_owned().into()).collect::<Vec<_>>()),
            filters: self.filters.map(|v| {
                let f = v.iter().map(|v| v.to_owned().into()).collect::<Vec<_>>();
                let r = EventGroupedFilters {
                    groups_condition: None,
                    groups: vec![EventGroupedFilterGroup {
                        filters_condition: Default::default(),
                        filters: f,
                    }],
                };
                r
            }),
        }
    }
}



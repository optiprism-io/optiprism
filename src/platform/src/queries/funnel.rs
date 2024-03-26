use std::sync::Arc;

use metadata::MetadataProvider;
use serde::Deserialize;
use serde::Serialize;

use crate::error::Result;
use crate::queries::event_records_search::EventRecordsSearchRequest;
use crate::queries::validation::validate_event;
use crate::queries::validation::validate_event_filter;
use crate::queries::validation::validate_property;
use crate::queries::Breakdown;
use crate::queries::QueryTime;
use crate::queries::Segment;
use crate::queries::TimeIntervalUnit;
use crate::EventFilter;
use crate::EventGroupedFilterGroup;
use crate::EventGroupedFilters;
use crate::EventRef;
use crate::PlatformError;
use crate::PropertyRef;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Funnel {
    pub time: QueryTime,
    pub group: String,
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

impl Into<common::query::funnel::Step> for Step {
    fn into(self) -> common::query::funnel::Step {
        common::query::funnel::Step {
            events: self
                .events
                .iter()
                .map(|e| e.to_owned().into())
                .collect::<Vec<_>>(),
            order: self.order.into(),
        }
    }
}

impl Into<Step> for common::query::funnel::Step {
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

impl Into<common::query::funnel::Order> for Order {
    fn into(self) -> common::query::funnel::Order {
        match self {
            Order::Any => common::query::funnel::Order::Any,
            Order::Exact => common::query::funnel::Order::Exact,
        }
    }
}

impl Into<Order> for common::query::funnel::Order {
    fn into(self) -> Order {
        match self {
            common::query::funnel::Order::Any => Order::Any,
            common::query::funnel::Order::Exact => Order::Exact,
        }
    }
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Event {
    #[serde(flatten)]
    pub event: EventRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<Vec<EventFilter>>,
}

impl Into<common::query::funnel::Event> for Event {
    fn into(self) -> common::query::funnel::Event {
        common::query::funnel::Event {
            event: self.event.into(),
            filters: self
                .filters
                .map(|f| f.iter().map(|e| e.to_owned().into()).collect::<Vec<_>>()),
        }
    }
}

impl Into<Event> for common::query::funnel::Event {
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

impl Into<common::query::funnel::TimeWindow> for TimeWindow {
    fn into(self) -> common::query::funnel::TimeWindow {
        common::query::funnel::TimeWindow {
            n: self.n,
            unit: self.unit.into(),
        }
    }
}

impl Into<TimeWindow> for common::query::funnel::TimeWindow {
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

impl Into<common::query::funnel::TimeIntervalUnitSession> for TimeIntervalUnitSession {
    fn into(self) -> common::query::funnel::TimeIntervalUnitSession {
        match self {
            TimeIntervalUnitSession::Hour => common::query::funnel::TimeIntervalUnitSession::Hour,
            TimeIntervalUnitSession::Day => common::query::funnel::TimeIntervalUnitSession::Day,
            TimeIntervalUnitSession::Week => common::query::funnel::TimeIntervalUnitSession::Week,
            TimeIntervalUnitSession::Month => common::query::funnel::TimeIntervalUnitSession::Month,
            TimeIntervalUnitSession::Year => common::query::funnel::TimeIntervalUnitSession::Year,
            TimeIntervalUnitSession::Session => {
                common::query::funnel::TimeIntervalUnitSession::Session
            }
        }
    }
}

impl Into<TimeIntervalUnitSession> for common::query::funnel::TimeIntervalUnitSession {
    fn into(self) -> TimeIntervalUnitSession {
        match self {
            common::query::funnel::TimeIntervalUnitSession::Hour => TimeIntervalUnitSession::Hour,
            common::query::funnel::TimeIntervalUnitSession::Day => TimeIntervalUnitSession::Day,
            common::query::funnel::TimeIntervalUnitSession::Week => TimeIntervalUnitSession::Week,
            common::query::funnel::TimeIntervalUnitSession::Month => TimeIntervalUnitSession::Month,
            common::query::funnel::TimeIntervalUnitSession::Year => TimeIntervalUnitSession::Year,
            common::query::funnel::TimeIntervalUnitSession::Session => {
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

impl Into<common::query::funnel::StepOrder> for StepOrder {
    fn into(self) -> common::query::funnel::StepOrder {
        match self {
            StepOrder::Exact => common::query::funnel::StepOrder::Exact,
            StepOrder::Any { steps } => common::query::funnel::StepOrder::Any(steps),
        }
    }
}

impl Into<StepOrder> for common::query::funnel::StepOrder {
    fn into(self) -> StepOrder {
        match self {
            common::query::funnel::StepOrder::Exact => StepOrder::Exact,
            common::query::funnel::StepOrder::Any(steps) => StepOrder::Any { steps },
        }
    }
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ExcludeSteps {
    All,
    Between { from: usize, to: usize },
}

impl Into<common::query::funnel::ExcludeSteps> for ExcludeSteps {
    fn into(self) -> common::query::funnel::ExcludeSteps {
        match self {
            ExcludeSteps::All => common::query::funnel::ExcludeSteps::All,
            ExcludeSteps::Between { from, to } => {
                common::query::funnel::ExcludeSteps::Between(from, to)
            }
        }
    }
}

impl Into<ExcludeSteps> for common::query::funnel::ExcludeSteps {
    fn into(self) -> ExcludeSteps {
        match self {
            common::query::funnel::ExcludeSteps::All => ExcludeSteps::All,
            common::query::funnel::ExcludeSteps::Between(from, to) => {
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

impl Into<common::query::funnel::Count> for Count {
    fn into(self) -> common::query::funnel::Count {
        match self {
            Count::Unique => common::query::funnel::Count::Unique,
            Count::NonUnique => common::query::funnel::Count::NonUnique,
            Count::Session => common::query::funnel::Count::Session,
        }
    }
}

impl Into<Count> for common::query::funnel::Count {
    fn into(self) -> Count {
        match self {
            common::query::funnel::Count::Unique => Count::Unique,
            common::query::funnel::Count::NonUnique => Count::NonUnique,
            common::query::funnel::Count::Session => Count::Session,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Exclude {
    pub event: Event,
    pub steps: Option<ExcludeSteps>,
}

impl Into<common::query::funnel::Exclude> for Exclude {
    fn into(self) -> common::query::funnel::Exclude {
        common::query::funnel::Exclude {
            event: self.event.into(),
            steps: self.steps.map(|s| s.into()),
        }
    }
}

impl Into<Exclude> for common::query::funnel::Exclude {
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

impl Into<common::query::funnel::Filter> for Filter {
    fn into(self) -> common::query::funnel::Filter {
        match self {
            Filter::DropOffOnAnyStep => common::query::funnel::Filter::DropOffOnAnyStep,
            Filter::DropOffOnStep { step } => common::query::funnel::Filter::DropOffOnStep(step),
            Filter::TimeToConvert { from, to } => {
                common::query::funnel::Filter::TimeToConvert(from, to)
            }
        }
    }
}

impl Into<Filter> for common::query::funnel::Filter {
    fn into(self) -> Filter {
        match self {
            common::query::funnel::Filter::DropOffOnAnyStep => Filter::DropOffOnAnyStep,
            common::query::funnel::Filter::DropOffOnStep(step) => Filter::DropOffOnStep { step },
            common::query::funnel::Filter::TimeToConvert(from, to) => {
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

impl Into<common::query::funnel::Touch> for Touch {
    fn into(self) -> common::query::funnel::Touch {
        match self {
            Touch::First => common::query::funnel::Touch::First,
            Touch::Last => common::query::funnel::Touch::Last,
            Touch::Step { step } => common::query::funnel::Touch::Step { step },
        }
    }
}

impl Into<Touch> for common::query::funnel::Touch {
    fn into(self) -> Touch {
        match self {
            common::query::funnel::Touch::First => Touch::First,
            common::query::funnel::Touch::Last => Touch::Last,
            common::query::funnel::Touch::Step { step } => Touch::Step { step },
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
impl Into<common::query::funnel::ChartType> for ChartType {
    fn into(self) -> common::query::funnel::ChartType {
        match self {
            ChartType::Steps => common::query::funnel::ChartType::Steps,
            ChartType::ConversionOverTime { interval_unit } => {
                common::query::funnel::ChartType::ConversionOverTime {
                    interval_unit: interval_unit.into(),
                }
            }
            ChartType::TimeToConvert {
                interval_unit,
                min_interval,
                max_interval,
            } => common::query::funnel::ChartType::TimeToConvert {
                interval_unit: interval_unit.into(),
                min_interval,
                max_interval,
            },
            ChartType::Frequency => common::query::funnel::ChartType::Frequency,
        }
    }
}

impl Into<ChartType> for common::query::funnel::ChartType {
    fn into(self) -> ChartType {
        match self {
            common::query::funnel::ChartType::Steps => ChartType::Steps,
            common::query::funnel::ChartType::ConversionOverTime { interval_unit } => {
                ChartType::ConversionOverTime {
                    interval_unit: interval_unit.into(),
                }
            }
            common::query::funnel::ChartType::TimeToConvert {
                interval_unit,
                min_interval,
                max_interval,
            } => ChartType::TimeToConvert {
                interval_unit: interval_unit.into(),
                min_interval,
                max_interval,
            },
            common::query::funnel::ChartType::Frequency => ChartType::Frequency,
        }
    }
}

impl Into<common::query::funnel::Funnel> for Funnel {
    fn into(self) -> common::query::funnel::Funnel {
        common::query::funnel::Funnel {
            time: self.time.into(),
            group: self.group.clone(),
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

impl Into<Funnel> for common::query::funnel::Funnel {
    fn into(self) -> Funnel {
        Funnel {
            time: self.time.into(),
            group: self.group.clone(),
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

pub(crate) fn validate(md: &Arc<MetadataProvider>, project_id: u64, req: &Funnel) -> Result<()> {
    match req.time {
        QueryTime::Between { from, to } => {
            if from > to {
                return Err(PlatformError::BadRequest(
                    "from time must be less than to time".to_string(),
                ));
            }
        }
        _ => {}
    }

    if req.steps.is_empty() {
        return Err(PlatformError::BadRequest(
            "steps must not be empty".to_string(),
        ));
    }

    for (step_id, step) in req.steps.iter().enumerate() {
        if step.events.is_empty() {
            return Err(PlatformError::BadRequest(format!(
                "step #{step_id}, events must not be empty"
            )));
        }

        for (event_id, event) in step.events.iter().enumerate() {
            validate_event(
                md,
                project_id,
                &event.event,
                event_id,
                format!("step #{step_id}, "),
            )?;

            match &event.filters {
                Some(filters) => {
                    if filters.is_empty() {
                        return Err(PlatformError::BadRequest(format!(
                            "step #{step_id}, filters must not be empty"
                        )));
                    }
                    for (filter_id, filter) in filters.iter().enumerate() {
                        validate_event_filter(
                            md,
                            project_id,
                            filter,
                            filter_id,
                            format!("event #{event_id}, "),
                        )?;
                    }
                }
                None => {}
            }
        }

        match &step.order {
            StepOrder::Exact => {}
            StepOrder::Any { steps } => steps
                .iter()
                .map(|(from, to)| {
                    if *from >= req.steps.len() {
                        return Err(PlatformError::BadRequest(
                            "step_order: from step index out of range".to_string(),
                        ));
                    }
                    if *to >= req.steps.len() {
                        return Err(PlatformError::BadRequest(
                            "step_order: to step index out of range".to_string(),
                        ));
                    }
                    Ok(())
                })
                .collect::<Result<_>>()?,
        }
    }

    if let Some(exclude) = &req.exclude {
        if exclude.is_empty() {
            return Err(PlatformError::BadRequest(
                "exclude must not be empty".to_string(),
            ));
        }
        for (exclude_id, exclude) in exclude.iter().enumerate() {
            validate_event(
                md,
                project_id,
                &exclude.event.event,
                exclude_id,
                format!("exclude #{exclude_id}, "),
            )?;
            match &exclude.steps {
                Some(steps) => match steps {
                    ExcludeSteps::Between { from, to } => {
                        if *from >= req.steps.len() {
                            return Err(PlatformError::BadRequest(
                                "exclude: from step index out of range".to_string(),
                            ));
                        }
                        if *to >= req.steps.len() {
                            return Err(PlatformError::BadRequest(
                                "exclude: to step index out of range".to_string(),
                            ));
                        }
                    }
                    ExcludeSteps::All => {}
                },
                None => {}
            }
        }
    }
    match &req.breakdowns {
        None => {}
        Some(breakdowns) => {
            if breakdowns.is_empty() {
                return Err(PlatformError::BadRequest(
                    "breakdowns must not be empty".to_string(),
                ));
            }
            for (idx, breakdown) in breakdowns.iter().enumerate() {
                match breakdown {
                    Breakdown::Property { property } => {
                        validate_property(md, project_id, property, format!("breakdown {idx}"))?;
                    }
                }
            }
        }
    }

    if req.segments.is_some() {
        return Err(PlatformError::Unimplemented(
            "segments are unimplemented yet".to_string(),
        ));
    }

    if let Some(hc) = &req.holding_constants {
        if hc.is_empty() {
            return Err(PlatformError::BadRequest(
                "holding constants field can't be empty".to_string(),
            ));
        }
        for (idx, prop) in hc.iter().enumerate() {
            validate_property(md, project_id, prop, format!("holding constant {idx}"))?;
        }
    }

    if let Some(filter) = &req.filter {
        match filter {
            Filter::DropOffOnAnyStep => {}
            Filter::DropOffOnStep { .. } => {}
            Filter::TimeToConvert { from, to } => {
                if from > to {
                    return Err(PlatformError::BadRequest(
                        "from time must be less than to time".to_string(),
                    ));
                }
            }
        }
    }

    match &req.filters {
        None => {}
        Some(filters) => {
            if filters.groups.is_empty() {
                return Err(PlatformError::BadRequest(
                    "groups field can't be empty".to_string(),
                ));
            }
            for filter_group in &filters.groups {
                if filters.groups.is_empty() {
                    return Err(PlatformError::BadRequest(
                        "filter_group field can't be empty".to_string(),
                    ));
                }
                if filter_group.filters.is_empty() {
                    return Err(PlatformError::BadRequest(
                        "filters field can't be empty".to_string(),
                    ));
                }
                for (filter_id, filter) in filter_group.filters.iter().enumerate() {
                    validate_event_filter(md, project_id, filter, filter_id, "".to_string())?;
                }
            }
        }
    }

    Ok(())
}

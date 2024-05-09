use std::sync::Arc;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use chronoutil::RelativeDuration;
use datafusion_common::Column;
use datafusion_expr::Expr;
use serde::Deserialize;
use serde::Serialize;

use crate::query::Breakdown;
use crate::query::EventRef;
use crate::query::PropValueFilter;
use crate::query::PropertyRef;
use crate::query::QueryTime;
use crate::query::Segment;
use crate::query::TimeIntervalUnit;
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Funnel {
    pub time: QueryTime,
    pub group_id: usize,
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
    pub filters: Option<Vec<PropValueFilter>>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Step {
    pub events: Vec<Event>,
    pub order: StepOrder,
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Order {
    Any,
    Exact,
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<PropValueFilter>>,
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TimeWindow {
    pub n: usize,
    pub unit: TimeIntervalUnitSession,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TimeIntervalUnitSession {
    Hour,
    Day,
    Week,
    Month,
    Year,
    Session,
}

impl TimeIntervalUnitSession {
    pub fn duration(&self, n: i64) -> Duration {
        match self {
            TimeIntervalUnitSession::Hour => Duration::hours(n),
            TimeIntervalUnitSession::Day => Duration::days(n),
            TimeIntervalUnitSession::Week => Duration::weeks(n),
            TimeIntervalUnitSession::Month => Duration::days(n * 31),
            TimeIntervalUnitSession::Year => Duration::days(n * 31 * 12),
            TimeIntervalUnitSession::Session => unimplemented!(),
        }
    }
    pub fn relative_duration(&self, n: i64) -> RelativeDuration {
        match self {
            TimeIntervalUnitSession::Hour => RelativeDuration::hours(n),
            TimeIntervalUnitSession::Day => RelativeDuration::days(n),
            TimeIntervalUnitSession::Week => RelativeDuration::weeks(n),
            TimeIntervalUnitSession::Month => RelativeDuration::months(n as i32),
            TimeIntervalUnitSession::Year => RelativeDuration::years(n as i32),
            TimeIntervalUnitSession::Session => unimplemented!(),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            TimeIntervalUnitSession::Hour => "hour",
            TimeIntervalUnitSession::Day => "day",
            TimeIntervalUnitSession::Week => "week",
            TimeIntervalUnitSession::Month => "month",
            TimeIntervalUnitSession::Year => "year",
            TimeIntervalUnitSession::Session => "session",
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum StepOrder {
    Exact,
    Any(Vec<(usize, usize)>), // any of the steps
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ExcludeSteps {
    All,
    Between(usize, usize),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Count {
    Unique,
    NonUnique,
    Session,
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Exclude {
    pub event: Event,
    pub steps: Option<ExcludeSteps>,
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Filter {
    // funnel should fail on any step
    DropOffOnAnyStep,
    // funnel should fail on certain step
    DropOffOnStep(usize),
    TimeToConvert(i64, i64), // conversion should be within certain window
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Touch {
    First,
    Last,
    Step { step: usize },
}
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ChartType {
    Steps,
    ConversionOverTime {
        interval_unit: TimeIntervalUnit,
    },
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

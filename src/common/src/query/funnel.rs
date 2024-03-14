use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use chronoutil::RelativeDuration;
use datafusion_common::Column;
use datafusion_expr::Expr;
use serde::Deserialize;
use serde::Serialize;

use crate::query::event_segmentation::Breakdown;
use crate::query::event_segmentation::Segment;
use crate::query::EventFilter;
use crate::query::EventRef;
use crate::query::PropertyRef;
use crate::query::QueryTime;
use crate::query::TimeIntervalUnit;

pub struct Funnel {
    pub time: QueryTime,
    pub group: String,
    pub steps: Vec<Step>,
    pub time_window: TimeWindow,
    pub chart_type: ChartType,
    pub count: Count,
    pub filter: Option<Filter>,
    pub touch: Touch,
    pub step_order: StepOrder,
    pub attribution: Option<Touch>,
    pub holding_constants: Option<Vec<PropertyRef>>,
    pub exclude: Option<Vec<Exclude>>,
    pub breakdowns: Option<Vec<Breakdown>>,
    pub segments: Option<Vec<Segment>>,
    pub filters: Option<Vec<EventFilter>>,
}

pub struct Step {
    pub events: Vec<Event>,
    pub order: StepOrder,
}

pub enum Order {
    Any,
    Exact,
}
pub struct Event {
    pub event: EventRef,
    pub filters: Option<Vec<EventFilter>>,
}

pub struct TimeWindow {
    pub n: i64,
    pub unit: TimeIntervalUnitSession,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum TimeIntervalUnitSession {
    Second,
    Minute,
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
            TimeIntervalUnitSession::Second => Duration::seconds(n),
            TimeIntervalUnitSession::Minute => Duration::minutes(n),
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
            TimeIntervalUnitSession::Second => RelativeDuration::seconds(n),
            TimeIntervalUnitSession::Minute => RelativeDuration::minutes(n),
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
            TimeIntervalUnitSession::Second => "second",
            TimeIntervalUnitSession::Minute => "minute",
            TimeIntervalUnitSession::Hour => "hour",
            TimeIntervalUnitSession::Day => "day",
            TimeIntervalUnitSession::Week => "week",
            TimeIntervalUnitSession::Month => "month",
            TimeIntervalUnitSession::Year => "year",
            TimeIntervalUnitSession::Session => "session",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum StepOrder {
    Sequential,
    Any(Vec<(usize, usize)>), // any of the steps
}

pub enum ExcludeSteps {
    All,
    Between(usize, usize),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ExcludeExpr {
    pub expr: Expr,
    pub steps: Option<Vec<ExcludeSteps>>,
    pub step_order: StepOrder,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Count {
    Unique,
    NonUnique,
    Session,
}
pub struct Exclude {
    pub event: Event,
    pub steps: Option<ExcludeSteps>,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Filter {
    DropOffOnAnyStep,
    // funnel should fail on any step
    DropOffOnStep(usize),
    // funnel should fail on certain step
    TimeToConvert(Duration, Duration), // conversion should be within certain window
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Touch {
    First,
    Last,
    Step(usize),
}

pub enum ChartType {
    Steps,
    ConversionOverTime {
        n: usize,
        interval_unit: TimeIntervalUnit,
    },
    TimeToConvert {
        interval_unit: TimeIntervalUnit,
        min_interval: i64,
        max_interval: i64,
    },
    Frequency,
}

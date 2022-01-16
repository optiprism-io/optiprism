use super::Event;
use super::Operator;
use crate::ifaces::query::PropertyOpValue;
use chrono::{Date, Duration, Utc};
use datafusion::scalar::ScalarValue;

pub enum Filter {
    DropOffOnAnyStep,
    DropOffOnStep(usize),
    TimeToConvert(Duration, Duration),
}

pub enum With {
    Count {
        op: Operator,
        value: usize,
    },
    RelativeCount {
        op: Operator,
        event_name: String,
    },
    PropertyValuesSum {
        property_name: String,
        op: Operator,
        value: f64,
    },
    PropertyDistinctValuesCount {
        property_name: String,
        op: Operator,
        value: usize,
    },
    Funnel {
        window: Duration,
        steps: Vec<Event>,
        exclude: Option<Vec<(Event, Vec<usize>)>>,
        user_constant_properties: Option<Vec<String>>,
        filter: Option<Filter>,
    },
}

pub enum TimeBucket {
    Hourly,
    Daily,
    Weekly,
    Monthly,
    Quarterly,
}

pub enum TimeWindow {
    LastDays(usize),
    Since(Date<Utc>),
    Between(Date<Utc>, Date<Utc>),
    Each(TimeBucket),
}

pub enum PerformedEvent {
    Any,
    Event(String),
}

pub enum Action {
    PerformedEvent {
        event: PerformedEvent,
        with: With,
        properties: Vec<PropertyOpValue>,
        window: TimeWindow,
    },
    HaveProperty {
        property: PropertyOpValue,
        window: TimeWindow,
    },
    HadProperty {
        property: PropertyOpValue,
        window: TimeWindow,
    },
}

pub enum Condition {
    And(Vec<Action>),
    AndNot(Vec<Action>),
}

pub struct UserSegment {
    conditions: Vec<Condition>,
}

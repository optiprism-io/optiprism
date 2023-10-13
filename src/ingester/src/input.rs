//! Ingester input definitions.

use std::collections::HashMap;
use std::net::IpAddr;

use chrono::DateTime;
use chrono::Utc;

/// A type to unify ingester input.
/// Basically for any type `T` where `IngesterInput: From<T>` we can run ingester pipeline.
#[derive(Debug)]
pub struct IngesterInput {
    pub user_id: String,
    pub event_name: String,
    pub event_properties: Option<Properties>,
    pub user_properties: Option<Properties>,
    pub context: Option<Context>,
    pub sent_at: Option<DateTime<Utc>>, /* TODO: should it be an input or maybe a processor thing? Like we know the time when request arrived */
    pub organization_id: u64,
    pub project_id: u64,
}

/// Event/User properties map coming from source.
pub type Properties = HashMap<String, Option<PropValue>>;

pub type Timestamp = i64;

/// Supported event/user property values.
#[derive(Debug)]
pub enum PropValue {
    String(String),
    Float64(f64),
    Bool(bool),
    Date(Timestamp),
}

/// Tracking context.
#[derive(Debug)]
pub struct Context {
    pub library: TrackingLibrary,
    //    pub page: Option<Page>,
    //    pub user_agent: String,
    pub ip: IpAddr,
}

#[derive(Debug)]
pub struct TrackingLibrary {
    pub name: String,
    pub version: String,
}

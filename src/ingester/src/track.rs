use std::collections::HashMap;
use std::net::Ipv4Addr;

use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::Context;
use crate::PropValue;
use crate::Property;

#[derive(Debug, Clone)]
pub struct Track {
    pub user_id: Option<String>,
    pub anonymous_id: Option<String>,
    pub resolved_user_id: Option<i64>,
    pub resolved_anonymous_user_id: Option<i64>,
    pub sent_at: DateTime<Utc>,
    pub context: Context,
    pub event: String,
    pub properties: HashMap<String, PropValue>,
    pub user_properties: HashMap<String, PropValue>,
    pub resolved_properties: Option<Vec<Property>>,
    pub resolved_user_properties: Option<Vec<Property>>,
}

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common::types::DType;
use error::Result;
use metadata::dictionaries::Dictionaries;
use metadata::events;
use metadata::groups::GroupValues;
use metadata::properties;
use metadata::properties::DictionaryType;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use storage::Value;

use crate::error::IngesterError;

pub mod destinations;
pub mod error;
pub mod executor;
pub mod sources;
pub mod transformers;

pub trait Transformer<T>: Send + Sync {
    fn process(&self, ctx: &RequestContext, req: T) -> Result<T>;
}

pub trait Destination<T>: Send + Sync {
    fn send(&self, ctx: &RequestContext, req: T) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub project_id: Option<u64>,
    pub client_ip: IpAddr,
    pub token: String,
}
#[derive(Debug, Clone)]
pub struct Campaign {
    pub source: String,
    pub medium: Option<String>,
    pub campaign: Option<String>,
    pub term: Option<String>,
    pub content: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Context {
    pub library: Option<Library>,
    pub page: Option<Page>,
    pub user_agent: Option<String>,
    pub ip: IpAddr,
    pub campaign: Option<Campaign>,
}

#[derive(Debug, Clone)]
pub struct Library {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone)]
pub struct Page {
    pub path: Option<String>,
    pub referrer: Option<String>,
    pub search: Option<String>,
    pub title: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PropertyAndValue {
    pub property: properties::Property,
    pub value: PropValue,
}
#[derive(Debug, Clone)]
pub struct Event {
    pub record_id: u64,
    pub event: events::Event,
}

#[derive(Debug, Clone)]
pub enum PropValue {
    Date(DateTime<Utc>),
    String(String),
    Number(Decimal),
    Bool(bool),
}

fn property_to_value(
    ctx: &RequestContext,
    prop: &PropertyAndValue,
    dict: &Arc<Dictionaries>,
) -> Result<Value> {
    let val = if prop.property.is_dictionary {
        if let PropValue::String(str_v) = &prop.value {
            let dict_id = dict.get_key_or_create(
                ctx.project_id.unwrap(),
                prop.property.column_name().as_str(),
                str_v.as_str(),
            )?;
            match prop.property.dictionary_type.clone().unwrap() {
                DictionaryType::Int8 => Value::Int8(Some(dict_id as i8)),
                DictionaryType::Int16 => Value::Int16(Some(dict_id as i16)),
                DictionaryType::Int32 => Value::Int32(Some(dict_id as i32)),
                DictionaryType::Int64 => Value::Int64(Some(dict_id as i64)),
            }
        } else {
            return Err(IngesterError::Internal(
                "property should be string".to_string(),
            ));
        }
    } else {
        match (&prop.property.data_type, &prop.value) {
            (DType::String, PropValue::String(v)) => Value::String(Some(v.to_owned())),
            (DType::Int8, PropValue::Number(v)) => Value::Int8(Some(v.to_i8().unwrap())), /* todo change to mantissa */
            (DType::Int16, PropValue::Number(v)) => Value::Int16(Some(v.to_i16().unwrap())),
            (DType::Int32, PropValue::Number(v)) => Value::Int32(Some(v.to_i32().unwrap())),
            (DType::Int64, PropValue::Number(v)) => Value::Int64(Some(v.to_i64().unwrap())),
            (DType::Decimal, PropValue::Number(v)) => Value::Decimal(Some(v.mantissa())),
            (DType::Boolean, PropValue::Bool(v)) => Value::Boolean(Some(*v)),
            (DType::Timestamp, PropValue::Date(v)) => Value::Int64(Some(v.timestamp())),
            _ => {
                return Err(IngesterError::Internal(
                    "property should be a string".to_string(),
                ));
            }
        }
    };

    Ok(val)
}

#[derive(Debug, Clone)]
pub struct Identify {
    pub timestamp: DateTime<Utc>,
    pub context: Context,
    pub group: String,
    pub group_id: u64,
    pub resolved_group_values: Option<GroupValues>,
    pub id: String,
    pub properties: Option<HashMap<String, PropValue>>,
    pub resolved_properties: Option<Vec<PropertyAndValue>>,
}

#[derive(Debug, Clone)]
pub struct Track {
    pub user_id: Option<String>,
    pub anonymous_id: Option<String>,
    pub resolved_user_id: Option<i64>,
    pub timestamp: DateTime<Utc>,
    pub context: Context,
    pub event: String,
    pub resolved_event: Option<events::Event>,
    pub properties: Option<HashMap<String, PropValue>>,
    pub resolved_properties: Option<Vec<PropertyAndValue>>,
    pub group_values: Option<HashMap<String, String>>,
    pub resolved_group_values: Option<Vec<(usize, GroupValues)>>,
}

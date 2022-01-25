use std::fmt::{Display, Formatter};
use arrow::array::{ListBuilder, StringBuilder, TimestampSecondBuilder, UInt64Builder};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bincode::deserialize;
use chrono::{DateTime, Utc};
use datafusion::scalar::ScalarValue;
use serde::{Deserialize, Serialize};
use common::types::RecordBatchWriter;
use crate::ColumnWriter;

pub trait IndexValues {
    fn status(&self) -> Status;
    fn project_id(&self) -> u64;
    fn name(&self) -> &str;
    fn display_name(&self) -> &Option<String>;
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Status {
    Enabled,
    Disabled,
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Enabled => write!(f, "enabled"),
            Status::Disabled => write!(f, "disabled"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Scope {
    System,
    User,
}

impl Display for Scope {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Scope::System => write!(f, "system"),
            Scope::User => write!(f, "user"),
        }
    }
}


#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Event {
    pub id: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub created_by: u64,
    pub updated_by: Option<u64>,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub scope: Scope,
    pub properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

impl IndexValues for Event {
    fn status(&self) -> Status {
        self.status.clone()
    }

    fn project_id(&self) -> u64 {
        self.project_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn display_name(&self) -> &Option<String> {
        &self.display_name
    }
}

pub struct ColumnWriter {
    id: UInt64Builder,
    created_at: TimestampSecondBuilder,
    updated_at: TimestampSecondBuilder,
    created_by: UInt64Builder,
    updated_by: UInt64Builder,
    project_id: UInt64Builder,
    tags: ListBuilder<StringBuilder>,
    name: StringBuilder,
    display_name: StringBuilder,
    description: StringBuilder,
    status: StringBuilder,
    scope: StringBuilder,
    properties: ListBuilder<UInt64Builder>,
    custom_properties: ListBuilder<UInt64Builder>,
    idx: Vec<usize>,

}

impl ColumnWriter {
    fn new(capacity: usize) -> Self {
        ColumnWriter {
            id: UInt64Builder::new(capacity),
            created_at: TimestampSecondBuilder::new(capacity),
            updated_at: TimestampSecondBuilder::new(capacity),
            created_by: UInt64Builder::new(capacity),
            updated_by: UInt64Builder::new(capacity),
            project_id: UInt64Builder::new(capacity),
            tags: ListBuilder::new(StringBuilder::new(capacity)),
            name: StringBuilder::new(capacity),
            display_name: StringBuilder::new(capacity),
            description: StringBuilder::new(capacity),
            status: StringBuilder::new(capacity),
            scope: StringBuilder::new(capacity),
            properties: ListBuilder::new(UInt64Builder::new(capacity)),
            custom_properties: ListBuilder::new(UInt64Builder::new(capacity)),
        }
    }
}

impl RecordBatchWriter for ColumnWriter {
    // TODO make procedural macro
    fn apply_schema(&mut self, schema: &Schema) {
        let mut ret = vec![];
        for field in schema.fields().iter() {
            match field.name().as_str() {
                "id" => ret.push(0),
                "created_at" => ret.push(1),
                "updated_at" => ret.push(2),
                "created_by" => ret.push(3),
                "updated_by" => ret.push(4),
                "project_id" => ret.push(5),
                "tags" => ret.push(6),
                "name" => ret.push(7),
                "display_name" => ret.push(8),
                "description" => ret.push(9),
                "status" => ret.push(10),
                "scope" => ret.push(11),
                "properties" => ret.push(12),
                "custom_properties" => ret.push(13),
                _ => unreachable!()
            }
        }

        self.idx = ret
    }

    fn write<T: AsRef<[u8]>>(&mut self, data: T) -> Result<()> {
        let event: Event = deserialize(data)?;
        for idx self.idx.iter() {
            match *idx {
                0=>self.id.append_value(event.id),
                1=>self.created_at.append_value(event.created_at.timestamp()),
                2=>self.updated_at.append_option(event.updated_at.map(|t|t.timestamp())),
                3=>self.created_by.append_value(event.created_by),
                4=>self.updated_by.append_option(event.updated_by),
                5=>self.project_id.append_value(event.project_id),
                6=>self.tags.append(event.project_id),
            }
        }
    }

    fn build(&self) -> RecordBatch {
        todo!()
    }
}

impl ColumnWriters for Event {
    fn values(&self, schema: &Schema) -> Vec<ScalarValue> {
        let a = UInt64Builder::new()
        schema.fields().iter().map(|f| {
            match f.name().as_str() {
                "id" => ScalarValue::UInt64(Some(self.id)),
                "created_at" => ScalarValue::TimestampSecond(Some(self.created_at.timestamp())),
                "updated_at" => ScalarValue::TimestampSecond(Some(self.updated_at.timestamp())),
                "created_by" => ScalarValue::UInt64(Some(self.created_by)),
                "updated_by" => ScalarValue::UInt64(self.updated_by),
                "project_id" => ScalarValue::UInt64(Some(self.project_id)),
                "tags" => ScalarValue::List(self.tags.map(|x| x.iter().map(|tag| ScalarValue::Utf8(Some(tag.to_owned().clone()))).collect()), Box::new(f.data_type().clone())),
                "name" => ScalarValue::Utf8(Some(self.name.clone())),
                "display_name" => ScalarValue::Utf8(self.display_name.clone()),
                "description" => ScalarValue::Utf8(self.description.clone()),
                "status" => ScalarValue::Utf8(Some(self.status.to_string())),
                "scope" => ScalarValue::Utf8(Some(self.scope.to_string())),
                "properties" => ScalarValue::List(self.properties.map(|x| x.iter().map(|prop_id| ScalarValue::UInt64(Some(*prop_id))).collect()), Box::new(f.data_type().clone())),
                "custom_properties" => ScalarValue::List(self.custom_properties.map(|x| x.iter().map(|prop_id| ScalarValue::UInt64(Some(*prop_id))).collect()), Box::new(f.data_type().clone())),
                _ => unreachable!()
            }
        }).collect()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CreateEventRequest {
    pub created_by: u64,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub scope: Scope,
    pub properties: Option<Vec<u64>>,
    pub global_properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

impl IndexValues for CreateEventRequest {
    fn status(&self) -> Status {
        self.status.clone()
    }

    fn project_id(&self) -> u64 {
        self.project_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn display_name(&self) -> &Option<String> {
        &self.display_name
    }
}

impl CreateEventRequest {
    pub fn into_event(self, id: u64, created_at: DateTime<Utc>) -> Event {
        Event {
            id,
            created_at,
            updated_at: None,
            created_by: self.created_by,
            updated_by: None,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            display_name: self.display_name,
            description: self.description,
            status: self.status,
            scope: self.scope,
            properties: self.properties,
            custom_properties: self.custom_properties,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct UpdateEventRequest {
    pub id: u64,
    pub created_by: u64,
    pub updated_by: u64,
    pub project_id: u64,
    pub tags: Option<Vec<String>>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub scope: Scope,
    pub properties: Option<Vec<u64>>,
    pub global_properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

impl IndexValues for UpdateEventRequest {
    fn status(&self) -> Status {
        self.status.clone()
    }

    fn project_id(&self) -> u64 {
        self.project_id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn display_name(&self) -> &Option<String> {
        &self.display_name
    }
}

impl UpdateEventRequest {
    pub fn into_event(self, prev: Event, updated_at: DateTime<Utc>, updated_by: Option<u64>) -> Event {
        Event {
            id: self.id,
            created_at: prev.created_at,
            updated_at: Some(updated_at),
            created_by: self.created_by,
            updated_by,
            project_id: self.project_id,
            tags: self.tags,
            name: self.name,
            display_name: self.display_name,
            description: self.description,
            status: self.status,
            scope: self.scope,
            properties: self.properties,
            custom_properties: self.custom_properties,
        }
    }
}
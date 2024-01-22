use std::sync::Arc;

use chrono::Utc;
use common::types::DType;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use common::types::COLUMN_USER_ID;
use common::types::EVENT_SESSION_BEGIN;
use metadata::dictionaries::Dictionaries;
use metadata::properties::DictionaryType;
use metadata::MetadataProvider;
use rust_decimal::prelude::ToPrimitive;
use storage::db::OptiDBImpl;
use storage::NamedValue;
use storage::Value;

use crate::error::IngesterError;
use crate::error::Result;
use crate::Destination;
use crate::PropValue;
use crate::PropertyAndValue;
use crate::RequestContext;
use crate::Track;

pub struct Local {
    db: Arc<OptiDBImpl>,
    md: Arc<MetadataProvider>,
}

impl Local {
    pub fn new(db: Arc<OptiDBImpl>, md: Arc<MetadataProvider>) -> Self {
        Self { db, md }
    }
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
            (DType::Int8, PropValue::Number(v)) => Value::Int8(Some(v.to_i8().unwrap())),
            (DType::Int16, PropValue::Number(v)) => Value::Int16(Some(v.to_i16().unwrap())),
            (DType::Int32, PropValue::Number(v)) => Value::Int32(Some(v.to_i32().unwrap())),
            (DType::Int64, PropValue::Number(v)) => Value::Int64(Some(v.to_i64().unwrap())),
            (DType::Decimal, PropValue::Number(v)) => Value::Decimal(Some(v.to_i128().unwrap())),
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

impl Destination<Track> for Local {
    fn send(&self, ctx: &RequestContext, req: Track) -> Result<()> {
        let ts = Utc::now();
        let is_new_session = self.md.sessions.set_current_time(
            ctx.project_id.unwrap(),
            req.resolved_user_id.unwrap() as u64,
            ts,
        )?;

        if is_new_session {
            let record_id = self
                .md
                .events
                .next_record_sequence(ctx.project_id.unwrap())?;

            let event_id = self
                .md
                .events
                .get_by_name(ctx.project_id.unwrap(), EVENT_SESSION_BEGIN)?
                .id;

            let values = vec![
                NamedValue::new(
                    COLUMN_PROJECT_ID.to_string(),
                    Value::Int64(Some(ctx.project_id.unwrap() as i64)),
                ),
                NamedValue::new(
                    COLUMN_USER_ID.to_string(),
                    Value::Int64(Some(req.resolved_user_id.unwrap())),
                ),
                NamedValue::new(
                    COLUMN_CREATED_AT.to_string(),
                    Value::Timestamp(Some(req.timestamp.timestamp())),
                ),
                NamedValue::new(
                    COLUMN_EVENT_ID.to_string(),
                    Value::Int64(Some(record_id as i64)),
                ),
                NamedValue::new(
                    COLUMN_EVENT.to_string(),
                    Value::Int64(Some(event_id as i64)),
                ),
            ];

            self.db.insert("events", values)?;
        }

        let record_id = self
            .md
            .events
            .next_record_sequence(ctx.project_id.unwrap())?;

        let event_id = req.resolved_event.as_ref().unwrap().id;

        let mut values = vec![
            NamedValue::new(
                COLUMN_PROJECT_ID.to_string(),
                Value::Int64(Some(ctx.project_id.unwrap() as i64)),
            ),
            NamedValue::new(
                COLUMN_USER_ID.to_string(),
                Value::Int64(Some(req.resolved_user_id.unwrap())),
            ),
            NamedValue::new(
                COLUMN_CREATED_AT.to_string(),
                Value::Timestamp(Some(req.timestamp.timestamp())),
            ),
            NamedValue::new(
                COLUMN_EVENT_ID.to_string(),
                Value::Int64(Some(record_id as i64)),
            ),
            NamedValue::new(
                COLUMN_EVENT.to_string(),
                Value::Int64(Some(event_id as i64)),
            ),
        ];

        let event_props = req
            .resolved_properties
            .as_ref()
            .cloned()
            .unwrap_or_else(Vec::new);

        for prop in &event_props {
            let value = property_to_value(ctx, prop, &self.md.dictionaries)?;
            values.push(NamedValue::new(prop.property.column_name(), value));
        }

        let user_props = req
            .resolved_properties
            .as_ref()
            .cloned()
            .unwrap_or_else(Vec::new);

        for prop in &user_props {
            let value = property_to_value(ctx, prop, &self.md.dictionaries)?;
            values.push(NamedValue::new(prop.property.column_name(), value));
        }
        self.db.insert("events", values)?;
        Ok(())
    }
}

use std::sync::Arc;

use chrono::Utc;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_REAL_TIMESTAMP;
use common::types::COLUMN_TIMESTAMP;
use common::types::COLUMN_USER_ID;
use futures::executor::block_on;
use metadata::dictionaries;
use metadata::properties;
use metadata::properties::DataType;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use rust_decimal::prelude::ToPrimitive;
use store::RowValue;
use store::SortedMergeTree;
use store::Value;
use store::ValueOp;

use crate::error::IngesterError;
use crate::error::Result;
use crate::Destination;
use crate::PropValue;
use crate::Property;
use crate::RequestContext;
use crate::Track;

pub struct Local {
    tbl: Arc<dyn SortedMergeTree>,
    dict: Arc<dyn dictionaries::Provider>,
    event_properties: Arc<dyn properties::Provider>,
    user_properties: Arc<dyn properties::Provider>,
}

impl Local {
    pub fn new(
        tbl: Arc<dyn SortedMergeTree>,
        dict: Arc<dyn dictionaries::Provider>,
        event_properties: Arc<dyn properties::Provider>,
        user_properties: Arc<dyn properties::Provider>,
    ) -> Self {
        Self {
            tbl,
            dict,

            event_properties,
            user_properties,
        }
    }
}

fn property_to_value(
    ctx: &RequestContext,
    prop: &properties::Property,
    event_prop: &Property,
    dict: &Arc<dyn dictionaries::Provider>,
) -> Result<Value> {
    let val = if prop.is_dictionary {
        if let PropValue::String(str_v) = &event_prop.value {
            let dict_id = dict.get_key_or_create(
                ctx.organization_id.unwrap(),
                ctx.project_id.unwrap(),
                prop.column_name().as_str(),
                str_v.as_str(),
            )?;
            match prop.dictionary_type.clone().unwrap() {
                DictionaryType::UInt8 => Value::UInt8(dict_id as u8),
                DictionaryType::UInt16 => Value::UInt16(dict_id as u16),
                DictionaryType::UInt32 => Value::UInt32(dict_id as u32),
                DictionaryType::UInt64 => Value::UInt64(dict_id),
            }
        } else {
            return Err(IngesterError::Internal(
                "property should be string".to_string(),
            ));
        }
    } else {
        match (&prop.data_type, &event_prop.value) {
            (DataType::String, PropValue::String(v)) => Value::String(v.to_owned()),
            (DataType::Int8, PropValue::Number(v)) => Value::Int8(v.to_i8().unwrap()),
            (DataType::Int16, PropValue::Number(v)) => Value::Int16(v.to_i16().unwrap()),
            (DataType::Int32, PropValue::Number(v)) => Value::Int32(v.to_i32().unwrap()),
            (DataType::Int64, PropValue::Number(v)) => Value::Int64(v.to_i64().unwrap()),
            (DataType::UInt8, PropValue::Number(v)) => Value::UInt8(v.to_u8().unwrap()),
            (DataType::UInt16, PropValue::Number(v)) => Value::UInt16(v.to_u16().unwrap()),
            (DataType::UInt32, PropValue::Number(v)) => Value::UInt32(v.to_u32().unwrap()),
            (DataType::UInt64, PropValue::Number(v)) => Value::UInt64(v.to_u64().unwrap()),
            (DataType::Float64, PropValue::Number(v)) => Value::Float64(v.to_f64().unwrap()),
            (DataType::Decimal, PropValue::Number(v)) => Value::Decimal(v.to_i128().unwrap()),
            (DataType::Boolean, PropValue::Bool(v)) => Value::Boolean(*v),
            (DataType::Timestamp, PropValue::Date(v)) => Value::Timestamp(*v),
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
        let mut values = Vec::new();

        values.push(RowValue::new(
            COLUMN_USER_ID.to_string(),
            Value::Int64(req.resolved_user_id.unwrap()),
        ));
        values.push(RowValue::new(
            COLUMN_TIMESTAMP.to_string(),
            Value::Timestamp(req.timestamp),
        ));
        values.push(RowValue::new(
            COLUMN_REAL_TIMESTAMP.to_string(),
            Value::Timestamp(Utc::now()),
        ));
        values.push(RowValue::new(
            COLUMN_EVENT_ID.to_string(),
            Value::UInt64(req.resolved_event.as_ref().unwrap().record_id),
        ));
        let event_id = req.resolved_event.as_ref().unwrap().event.id;

        values.push(RowValue::new(
            COLUMN_EVENT.to_string(),
            Value::UInt16(event_id as u16),
        ));
        let event_res = self
            .event_properties
            .list(ctx.organization_id.unwrap(), ctx.project_id.unwrap())?;

        let user_res = self
            .user_properties
            .list(ctx.organization_id.unwrap(), ctx.project_id.unwrap())?;

        let event_props = req
            .resolved_properties
            .map(|v| v.clone())
            .unwrap_or_else(|| vec![]);

        for prop in event_res.data {
            let mut found = false;
            match prop.typ {
                Type::Event => {
                    for event_prop in &event_props {
                        if event_prop.property.id == prop.id {
                            found = true;
                            let value = property_to_value(ctx, &prop, &event_prop, &self.dict)?;
                            values.push(RowValue::new(prop.column_name(), value));
                        };
                    }
                }
                _ => {}
            }
            if !found {
                values.push(RowValue::new(prop.column_name(), Value::Null));
            }
        }
        let user_props = req
            .resolved_user_properties
            .map(|v| v.clone())
            .unwrap_or_else(|| vec![]);
        for prop in user_res.data {
            let mut found = false;
            match prop.typ {
                Type::Event => {
                    for event_prop in &user_props {
                        if event_prop.property.id == prop.id {
                            found = true;
                            let value = property_to_value(ctx, &prop, &event_prop, &self.dict)?;
                            values.push(RowValue::new(prop.column_name(), value));
                        };
                    }
                }
                _ => {}
            }
            if !found {
                values.push(RowValue::new(prop.column_name(), Value::Null));
            }
        }

        self.tbl.insert(values)?;
        Ok(())
    }
}

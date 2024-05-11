use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use common::group_col;
use common::types::DType;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_IP;
use common::types::COLUMN_PROJECT_ID;
use common::types::EVENT_SESSION_BEGIN;
use common::GROUPS_COUNT;
use common::GROUP_USER_ID;
use metadata::dictionaries::Dictionaries;
use metadata::properties::DictionaryType;
use metadata::MetadataProvider;
use rust_decimal::prelude::ToPrimitive;
use storage::db::OptiDBImpl;
use storage::NamedValue;
use storage::Value;

use crate::error::IngesterError;
use crate::error::Result;
use crate::property_to_value;
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

impl Destination<Track> for Local {
    fn send(&self, ctx: &RequestContext, req: Track) -> Result<()> {
        let ts = Utc::now();
        let is_new_session = self.md.sessions.set_current_time(
            ctx.project_id.unwrap(),
            req.resolved_user_id.unwrap() as u64,
            ts,
        )?;

        // fill group id for each of GROUPS_COUNT groups
        let groups = if let Some(groups) = &req.resolved_groups {
            let mut found = HashMap::new();
            let mut groups = groups
                .iter()
                .map(|(group_id, group)| {
                    found.insert(*group_id, ());
                    NamedValue::new(group_col(*group_id), Value::Int64(Some(group.id as i64)))
                })
                .collect::<Vec<_>>();

            for g in 1..GROUPS_COUNT {
                if !found.contains_key(&g) {
                    groups.push(NamedValue::new(group_col(g), Value::Int64(Some(0))))
                }
            }
            groups
        } else {
            // skip user group, i.e. skip 0
            (1..GROUPS_COUNT)
                .into_iter()
                .map(|v| NamedValue::new(group_col(v), Value::Int64(Some(0)))) // don't use null as primary key, only zero
                .collect::<Vec<_>>()
        };

        let props = req
            .resolved_properties
            .as_ref()
            .cloned()
            .unwrap_or_else(Vec::new);

        let mut prop_values = vec![];
        for prop in &props {
            let value = property_to_value(ctx, prop, &self.md.dictionaries)?;
            prop_values.push(NamedValue::new(prop.property.column_name(), value));
        }

        if let Some(resolved_groups) = &req.resolved_groups {
            for (group_id, group) in resolved_groups {
                for value in &group.values {
                    let prop = self.md.group_properties[*group_id]
                        .get_by_id(ctx.project_id.unwrap(), value.property_id)?;
                    let value = match value.value.clone() {
                        metadata::groups::Value::Null => Value::Null,
                        metadata::groups::Value::Int8(v) => Value::Int8(v),
                        metadata::groups::Value::Int16(v) => Value::Int16(v),
                        metadata::groups::Value::Int32(v) => Value::Int32(v),
                        metadata::groups::Value::Int64(v) => Value::Int64(v),
                        metadata::groups::Value::Boolean(v) => Value::Boolean(v),
                        metadata::groups::Value::Timestamp(v) => Value::Timestamp(v),
                        metadata::groups::Value::Decimal(v) => Value::Decimal(v),
                        metadata::groups::Value::String(v) => Value::String(v),
                        metadata::groups::Value::ListInt8(v) => Value::ListInt8(v),
                        metadata::groups::Value::ListInt16(v) => Value::ListInt16(v),
                        metadata::groups::Value::ListInt32(v) => Value::ListInt32(v),
                        metadata::groups::Value::ListInt64(v) => Value::ListInt64(v),
                        metadata::groups::Value::ListBoolean(v) => Value::ListBoolean(v),
                        metadata::groups::Value::ListTimestamp(v) => Value::ListTimestamp(v),
                        metadata::groups::Value::ListDecimal(v) => Value::ListDecimal(v),
                        metadata::groups::Value::ListString(v) => Value::ListString(v),
                    };
                    prop_values.push(NamedValue::new(prop.column_name(), value));
                }
            }
        }

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

            let mut values = vec![
                vec![
                    NamedValue::new(
                        COLUMN_PROJECT_ID.to_string(),
                        Value::Int64(Some(ctx.project_id.unwrap() as i64)),
                    ),
                    NamedValue::new(
                        group_col(GROUP_USER_ID),
                        Value::Int64(Some(req.resolved_user_id.unwrap())),
                    ),
                ],
                groups.clone(),
                vec![
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
                    NamedValue::new(
                        COLUMN_IP.to_string(),
                        Value::String(Some(req.context.ip.to_string())),
                    ),
                ],
                prop_values.clone(),
            ]
            .concat();

            self.db.insert("events", values)?;
        }

        let record_id = self
            .md
            .events
            .next_record_sequence(ctx.project_id.unwrap())?;

        let event_id = req.resolved_event.as_ref().unwrap().id;

        let mut values = vec![
            vec![
                NamedValue::new(
                    COLUMN_PROJECT_ID.to_string(),
                    Value::Int64(Some(ctx.project_id.unwrap() as i64)),
                ),
                NamedValue::new(
                    group_col(GROUP_USER_ID),
                    Value::Int64(Some(req.resolved_user_id.unwrap())),
                ),
            ],
            groups,
            vec![
                NamedValue::new(
                    COLUMN_CREATED_AT.to_string(),
                    Value::Timestamp(Some(req.timestamp.timestamp_millis())),
                ),
                NamedValue::new(
                    COLUMN_EVENT_ID.to_string(),
                    Value::Int64(Some(record_id as i64)),
                ),
                NamedValue::new(
                    COLUMN_EVENT.to_string(),
                    Value::Int64(Some(event_id as i64)),
                ),
            ],
            prop_values.clone(),
        ]
        .concat();

        let user_props = req
            .resolved_user_properties
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

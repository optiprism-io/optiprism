use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use common::group_col;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_IP;
use common::types::COLUMN_PROJECT_ID;
use common::types::EVENT_PROPERTY_UTM_CAMPAIGN;
use common::types::EVENT_PROPERTY_UTM_CONTENT;
use common::types::EVENT_PROPERTY_UTM_MEDIUM;
use common::types::EVENT_PROPERTY_UTM_SOURCE;
use common::types::EVENT_PROPERTY_UTM_TERM;
use common::types::EVENT_SESSION_BEGIN;
use common::types::TABLE_EVENTS;
use common::GROUPS_COUNT;
use common::GROUP_USER_ID;
use metadata::MetadataProvider;
use storage::db::OptiDBImpl;
use storage::NamedValue;
use storage::Value;

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
        let groups = if let Some(groups) = &req.resolved_group_values {
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
                    groups.push(NamedValue::new(group_col(g), Value::Int64(None)))
                }
            }
            groups
        } else {
            // skip user group, i.e. skip 0
            (1..GROUPS_COUNT)
                .map(|v| NamedValue::new(group_col(v), Value::Int64(None))) // don't use null as primary key, only zero
                .collect::<Vec<_>>()
        };

        let props = req
            .resolved_properties
            .as_ref()
            .cloned()
            .unwrap_or_else(Vec::new);

        let mut prop_values = vec![];
        for prop in &props {
            let value = property_to_value(ctx, TABLE_EVENTS, prop, &self.md.dictionaries)?;
            prop_values.push(NamedValue::new(prop.property.column_name(), value));
        }

        if let Some(resolved_groups) = &req.resolved_group_values {
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

        let utm = if let Some(campaign) = &req.context.campaign {
            let mut utm = vec![];
            let property = self
                .md
                .event_properties
                .get_by_name(ctx.project_id.unwrap(), EVENT_PROPERTY_UTM_SOURCE)?;
            let value = PropValue::String(campaign.source.clone());

            let value = property_to_value(
                ctx,
                TABLE_EVENTS,
                &PropertyAndValue {
                    property: property.clone(),
                    value,
                },
                &self.md.dictionaries,
            )?;
            utm.push(NamedValue::new(property.column_name(), value));
            if let Some(medium) = &campaign.medium {
                let property = self
                    .md
                    .event_properties
                    .get_by_name(ctx.project_id.unwrap(), EVENT_PROPERTY_UTM_MEDIUM)?;
                let value = PropValue::String(medium.to_owned());

                let value = property_to_value(
                    ctx,
                    TABLE_EVENTS,
                    &PropertyAndValue {
                        property: property.clone(),
                        value,
                    },
                    &self.md.dictionaries,
                )?;
                utm.push(NamedValue::new(property.column_name(), value));
            }

            if let Some(campaign) = &campaign.campaign {
                let property = self
                    .md
                    .event_properties
                    .get_by_name(ctx.project_id.unwrap(), EVENT_PROPERTY_UTM_CAMPAIGN)?;
                let value = PropValue::String(campaign.to_owned());
                let value = property_to_value(
                    ctx,
                    TABLE_EVENTS,
                    &PropertyAndValue {
                        property: property.clone(),
                        value,
                    },
                    &self.md.dictionaries,
                )?;
                utm.push(NamedValue::new(property.column_name(), value));
            }

            if let Some(term) = &campaign.term {
                let property = self
                    .md
                    .event_properties
                    .get_by_name(ctx.project_id.unwrap(), EVENT_PROPERTY_UTM_TERM)?;
                let value = PropValue::String(term.to_owned());
                let value = property_to_value(
                    ctx,
                    TABLE_EVENTS,
                    &PropertyAndValue {
                        property: property.clone(),
                        value,
                    },
                    &self.md.dictionaries,
                )?;
                utm.push(NamedValue::new(property.column_name(), value));
            }

            if let Some(content) = &campaign.content {
                let property = self
                    .md
                    .event_properties
                    .get_by_name(ctx.project_id.unwrap(), EVENT_PROPERTY_UTM_CONTENT)?;
                let value = PropValue::String(content.to_owned());
                let value = property_to_value(
                    ctx,
                    TABLE_EVENTS,
                    &PropertyAndValue {
                        property: property.clone(),
                        value,
                    },
                    &self.md.dictionaries,
                )?;
                utm.push(NamedValue::new(property.column_name(), value));
            }

            utm
        } else {
            vec![]
        };
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

            let values = [
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
                utm,
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

            self.db.insert(TABLE_EVENTS, values)?;
        }

        let record_id = self
            .md
            .events
            .next_record_sequence(ctx.project_id.unwrap())?;

        let event_id = req.resolved_event.as_ref().unwrap().id;

        let values = [
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

        self.db.insert(TABLE_EVENTS, values)?;
        Ok(())
    }
}

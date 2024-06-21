use std::collections::HashMap;
use std::sync::Arc;

use common::group_col;
use common::types::DType;
use common::GROUP_USER_ID;
use metadata::events;
use metadata::events::CreateEventRequest;
use metadata::properties;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::DictionaryType;
use metadata::properties::Properties;
use metadata::properties::Status;
use metadata::MetadataProvider;
use storage::db::OptiDBImpl;
use storage::Value;

use crate::error::IngesterError;
use crate::error::Result;
use crate::property_to_value;
use crate::Destination;
use crate::Identify;
use crate::PropValue;
use crate::PropertyAndValue;
use crate::RequestContext;
use crate::Track;
use crate::Transformer;

fn resolve_property(
    ctx: &RequestContext,
    properties: &Arc<Properties>,
    _db: &Arc<OptiDBImpl>,
    typ: properties::Type,
    name: String,
    val: PropValue,
) -> Result<PropertyAndValue> {
    let data_type = match val {
        PropValue::Date(_) => DType::Int64,
        PropValue::String(_) => DType::String,
        PropValue::Number(_) => DType::Decimal,
        PropValue::Bool(_) => DType::Boolean,
    };

    let (is_dictionary, dictionary_type) = if let PropValue::String(_) = val {
        (true, Some(DictionaryType::Int64))
    } else {
        (false, None)
    };
    let req = CreatePropertyRequest {
        created_by: 1,
        tags: None,
        name: name.clone(),
        description: None,
        display_name: None,
        typ: typ.clone(),
        data_type,
        status: Status::Enabled,
        hidden: false,
        is_system: false,
        nullable: true,
        is_array: false,
        is_dictionary,
        dictionary_type,
    };

    let prop = properties.get_or_create(ctx.project_id.unwrap(), req)?;

    Ok(PropertyAndValue {
        property: prop,
        value: val.clone(),
    })
}

fn resolve_properties(
    ctx: &RequestContext,
    properties: &Arc<Properties>,
    db: &Arc<OptiDBImpl>,
    typ: properties::Type,
    props: &HashMap<String, PropValue>,
) -> Result<Vec<PropertyAndValue>> {
    let resolved_props = props
        .iter()
        .map(|(k, v)| resolve_property(ctx, properties, db, typ.clone(), k.to_owned(), v.clone()))
        .collect::<Result<Vec<PropertyAndValue>>>()?;
    Ok(resolved_props)
}

pub struct Executor<T> {
    transformers: Vec<Arc<dyn Transformer<T>>>,
    destinations: Vec<Arc<dyn Destination<T>>>,
    db: Arc<OptiDBImpl>,
    md: Arc<MetadataProvider>,
}

#[allow(clippy::too_many_arguments)]
impl Executor<Track> {
    pub fn new(
        transformers: Vec<Arc<dyn Transformer<Track>>>,
        destinations: Vec<Arc<dyn Destination<Track>>>,
        db: Arc<OptiDBImpl>,
        md: Arc<MetadataProvider>,
    ) -> Self {
        Self {
            transformers,
            destinations,
            db,
            md,
        }
    }

    pub fn execute(&self, ctx: &RequestContext, mut req: Track) -> Result<()> {
        let project = self.md.projects.get_by_token(ctx.token.as_str())?;
        let mut ctx = ctx.to_owned();
        ctx.project_id = Some(project.id);
        if let Some(props) = &req.properties {
            req.resolved_properties = Some(resolve_properties(
                &ctx,
                &self.md.event_properties,
                &self.db,
                properties::Type::Event,
                props,
            )?);
        }
        let (user_id, user_group) = match (&req.user_id, &req.anonymous_id) {
            (Some(user_id), None) => {
                let group = self.md.groups.get_or_create(
                    ctx.project_id.unwrap(),
                    GROUP_USER_ID as u64,
                    user_id,
                    vec![],
                )?;
                self.md.dictionaries.create_key(
                    ctx.project_id.unwrap(),
                    group_col(GROUP_USER_ID).as_str(),
                    group.id,
                    user_id.as_str(),
                )?;

                (group.id, Some(group))
            }
            (None, Some(user_id)) => {
                let id = self.md.groups.get_or_create_anonymous_id(
                    ctx.project_id.unwrap(),
                    GROUP_USER_ID as u64,
                    user_id.as_str(),
                )?;

                self.md.dictionaries.create_key(
                    ctx.project_id.unwrap(),
                    group_col(GROUP_USER_ID).as_str(),
                    id,
                    user_id.as_str(),
                )?;

                (id, None)
            }
            (Some(aid), Some(uid)) => {
                let group = self.md.groups.merge_with_anonymous(
                    ctx.project_id.unwrap(),
                    GROUP_USER_ID as u64,
                    aid.as_str(),
                    uid.as_str(),
                    vec![],
                )?;
                (group.id, None)
            }
            _ => {
                return Err(IngesterError::BadRequest(
                    "user_id or anonymous_id must be set".to_string(),
                ));
            }
        };

        req.resolved_user_id = Some(user_id as i64);

        if let Some(groups) = &req.group_values {
            let mut resolved_groups = vec![];
            // add user as a first group
            if let Some(user_group) = user_group {
                resolved_groups.push((0, user_group));
            }

            for (group_name, group_val) in groups {
                let group = self.md.groups.get_or_create_group(
                    ctx.project_id.unwrap(),
                    group_name.to_owned(),
                    group_name.to_owned(),
                )?;

                let resolved_group = self.md.groups.get_or_create(
                    ctx.project_id.unwrap(),
                    group.id,
                    group_val,
                    vec![],
                )?;

                resolved_groups.push((group.id as usize, resolved_group));
            }
            req.resolved_group_values = Some(resolved_groups);
        }
        let event_req = CreateEventRequest {
            created_by: 1,
            tags: None,
            name: req.event.to_owned(),
            description: None,
            display_name: None,
            status: events::Status::Enabled,
            is_system: false,
            event_properties: req
                .resolved_properties
                .clone()
                .map(|props| props.iter().map(|p| p.property.id).collect::<Vec<u64>>()),
            user_properties: None,
            custom_properties: None,
        };
        let md_event = self
            .md
            .events
            .get_or_create(ctx.project_id.unwrap(), event_req)?;
        let event_id = md_event.id;
        req.resolved_event = Some(md_event);
        if let Some(props) = &req.resolved_properties {
            self.md.events.try_attach_properties(
                ctx.project_id.unwrap(),
                event_id,
                props.iter().map(|p| p.property.id).collect(),
            )?;
        }

        for transformer in &self.transformers {
            req = transformer.process(&ctx, req)?;
        }
        for dest in &self.destinations {
            dest.send(&ctx, req.clone())?;
        }

        self.md
            .projects
            .increment_events_counter(ctx.project_id.unwrap())?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
impl Executor<Identify> {
    pub fn new(
        transformers: Vec<Arc<dyn Transformer<Identify>>>,
        destinations: Vec<Arc<dyn Destination<Identify>>>,
        db: Arc<OptiDBImpl>,
        md: Arc<MetadataProvider>,
    ) -> Self {
        Self {
            transformers,
            destinations,
            db,
            md,
        }
    }

    pub fn execute(&self, ctx: &RequestContext, mut req: Identify) -> Result<()> {
        let ctx = ctx.to_owned();
        let project = self.md.projects.get_by_token(ctx.token.as_str())?;
        let mut ctx = ctx.to_owned();
        ctx.project_id = Some(project.id);

        let group = self.md.groups.get_or_create_group(
            ctx.project_id.unwrap(),
            req.group.to_owned(),
            req.group.to_owned(),
        )?;

        if let Some(props) = &req.properties {
            req.resolved_properties = Some(resolve_properties(
                &ctx,
                &self.md.group_properties[group.id as usize],
                &self.db,
                properties::Type::Group(group.id as usize),
                props,
            )?);
        }

        let vals = if let Some(props) = &req.resolved_properties {
            let mut vals = vec![];
            for prop in props {
                let v = property_to_value(&ctx, prop, &self.md.dictionaries)?;
                let vv = match v {
                    Value::Int8(v) => metadata::groups::Value::Int8(v),
                    Value::Int16(v) => metadata::groups::Value::Int16(v),
                    Value::Int32(v) => metadata::groups::Value::Int32(v),
                    Value::Int64(v) => metadata::groups::Value::Int64(v),
                    Value::Boolean(v) => metadata::groups::Value::Boolean(v),
                    Value::Timestamp(v) => metadata::groups::Value::Timestamp(v),
                    Value::Decimal(v) => metadata::groups::Value::Decimal(v),
                    Value::String(v) => metadata::groups::Value::String(v),
                    Value::ListInt8(v) => metadata::groups::Value::ListInt8(v),
                    Value::ListInt16(v) => metadata::groups::Value::ListInt16(v),
                    Value::ListInt32(v) => metadata::groups::Value::ListInt32(v),
                    Value::ListInt64(v) => metadata::groups::Value::ListInt64(v),
                    Value::ListBoolean(v) => metadata::groups::Value::ListBoolean(v),
                    Value::ListTimestamp(v) => metadata::groups::Value::ListTimestamp(v),
                    Value::ListDecimal(v) => metadata::groups::Value::ListDecimal(v),
                    Value::ListString(v) => metadata::groups::Value::ListString(v),
                    _ => {
                        return Err(IngesterError::BadRequest(
                            "unsupported value type".to_string(),
                        ));
                    }
                };

                vals.push(metadata::groups::PropertyValue {
                    property_id: prop.property.id,
                    value: vv,
                })
            }

            vals
        } else {
            vec![]
        };


        let resolved_group_values = self.md.groups.create_or_update(
            ctx.project_id.unwrap(),
            group.id,
            req.id.as_str(),
            vals,
        )?;
        let group_vals_id = resolved_group_values.id;
        req.group_id = group.id;
        req.resolved_group_values = Some(resolved_group_values);

        // create dict for each group values so we can use it in property values
        self.md.dictionaries.create_key(
            ctx.project_id.unwrap(),
            group_col(req.group_id as usize).as_str(),
            group_vals_id,
            req.id.as_str(),
        )?;
        // if req.group_id == 0 {
        //     print!("{group_vals_id} - {} ", req.id);
        // }

        for transformer in &self.transformers {
            req = transformer.process(&ctx, req)?;
        }

        for dest in &self.destinations {
            dest.send(&ctx, req.clone())?;
        }
        Ok(())
    }
}

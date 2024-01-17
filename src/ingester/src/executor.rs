use std::collections::HashMap;
use std::sync::Arc;

use common::types::DType;
use common::types::DICT_USERS;
use metadata::events;
use metadata::events::CreateEventRequest;
use metadata::properties;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::DictionaryType;
use metadata::properties::Properties;
use metadata::properties::Status;
use metadata::MetadataProvider;
use store::db::OptiDBImpl;

use crate::error::IngesterError;
use crate::error::Result;
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

    pub fn execute(&mut self, ctx: &RequestContext, mut req: Track) -> Result<()> {
        let project = self.md.projects.get_by_token(ctx.token.as_str())?;
        let mut ctx = ctx.to_owned();
        ctx.project_id = Some(project.id);

        let user_id = match (&req.user_id, &req.anonymous_id) {
            (Some(user_id), None) => self.md.dictionaries.get_key_or_create(
                ctx.project_id.unwrap(),
                DICT_USERS,
                user_id.as_str(),
            )?,
            (None, Some(user_id)) => self.md.dictionaries.get_key_or_create(
                ctx.project_id.unwrap(),
                DICT_USERS,
                user_id.as_str(),
            )?,
            _ => {
                return Err(IngesterError::BadRequest(
                    "user_id or anonymous_id must be set".to_string(),
                ));
            }
        };

        req.resolved_user_id = Some(user_id as i64);
        if let Some(props) = &req.properties {
            req.resolved_properties = Some(resolve_properties(
                &ctx,
                &self.md.event_properties,
                &self.db,
                properties::Type::Event,
                props,
            )?);
        }

        if let Some(props) = &req.user_properties {
            req.resolved_user_properties = Some(resolve_properties(
                &ctx,
                &self.md.user_properties,
                &self.db,
                properties::Type::Event,
                props,
            )?);
        }

        let event_req = CreateEventRequest {
            created_by: 1,
            tags: None,
            name: req.event.to_owned(),
            description: None,
            display_name: None,
            status: events::Status::Enabled,
            is_system: false,
            properties: req
                .resolved_properties
                .clone()
                .map(|props| props.iter().map(|p| p.property.id).collect::<Vec<u64>>()),
            custom_properties: None,
        };

        let md_event = self
            .md
            .events
            .get_or_create(ctx.project_id.unwrap(), event_req)?;

        req.resolved_event = Some(md_event);

        for transformer in &mut self.transformers {
            req = transformer.process(&ctx, req)?;
        }

        for dest in &mut self.destinations {
            dest.send(&ctx, req.clone())?;
        }

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

    pub fn execute(&mut self, ctx: &RequestContext, mut req: Identify) -> Result<()> {
        let ctx = ctx.to_owned();
        let project = self.md.projects.get_by_token(ctx.token.as_str())?;
        let mut ctx = ctx.to_owned();
        ctx.project_id = Some(project.id);

        for transformer in &mut self.transformers {
            req = transformer.process(&ctx, req)?;
        }

        for dest in &mut self.destinations {
            dest.send(&ctx, req.clone())?;
        }
        Ok(())
    }
}

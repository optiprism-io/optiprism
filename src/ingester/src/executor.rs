use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use futures::executor::block_on;
use metadata::events;
use metadata::events::CreateEventRequest;
use metadata::projects;
use metadata::properties;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::DataType;
use metadata::properties::DictionaryType;
use metadata::properties::Status;

use crate::error::Result;
use crate::Destination;
use crate::Event;
use crate::Identify;
use crate::Processor;
use crate::PropValue;
use crate::Property;
use crate::RequestContext;
use crate::Track;

fn resolve_property(
    ctx: &RequestContext,
    properties: &Arc<dyn properties::Provider>,
    typ: properties::Type,
    name: String,
    val: &PropValue,
) -> Result<Property> {
    let dt = match val {
        PropValue::Date(_) => DataType::Timestamp,
        PropValue::String(_) => DataType::String,
        PropValue::Number(_) => DataType::Decimal,
        PropValue::Bool(_) => DataType::Boolean,
    };

    let (is_dictionary, dictionary_type) = if let PropValue::String(_) = val {
        (true, Some(DictionaryType::UInt64))
    } else {
        (false, None)
    };
    let req = CreatePropertyRequest {
        created_by: 1,
        tags: None,
        name: name.clone(),
        description: None,
        display_name: None,
        typ,
        data_type: DataType::String,
        status: Status::Enabled,
        is_system: false,
        nullable: true,
        is_array: false,
        is_dictionary,
        dictionary_type,
    };

    let prop = block_on(properties.get_or_create(
        ctx.organization_id.unwrap(),
        ctx.project_id.unwrap(),
        req,
    ))?;
    Ok(Property {
        property: prop,
        value: val.to_owned().into(),
    })
}

fn resolve_properties(
    ctx: &RequestContext,
    properties: &Arc<dyn properties::Provider>,
    typ: properties::Type,
    props: &HashMap<String, PropValue>,
) -> Result<Vec<Property>> {
    let resolved_props = props
        .iter()
        .map(|(k, v)| resolve_property(&ctx, properties, typ.clone(), k.to_owned(), v))
        .collect::<Result<Vec<Property>>>()?;
    Ok(resolved_props)
}

pub struct Executor<T> {
    processors: Vec<Arc<dyn Processor<T>>>,
    destinations: Vec<Arc<dyn Destination<T>>>,
    event_properties: Arc<dyn properties::Provider>,
    user_properties: Arc<dyn properties::Provider>,
    events: Arc<dyn events::Provider>,
    projects: Arc<dyn projects::Provider>,
}

impl Executor<Track> {
    pub fn new(
        processors: Vec<Arc<dyn Processor<Track>>>,
        destinations: Vec<Arc<dyn Destination<Track>>>,
        event_properties: Arc<dyn properties::Provider>,
        user_properties: Arc<dyn properties::Provider>,
        events: Arc<dyn events::Provider>,
        projects: Arc<dyn projects::Provider>,
    ) -> Self {
        Self {
            processors,
            destinations,
            event_properties,
            user_properties,
            events,
            projects,
        }
    }

    pub fn execute(&mut self, ctx: &RequestContext, mut req: Track) -> Result<()> {
        let project = block_on(self.projects.get_by_token(ctx.token.as_str()))?;
        let mut ctx = ctx.to_owned();
        ctx.organization_id = Some(project.organization_id);
        ctx.project_id = Some(project.id);

        if let Some(props) = &req.properties {
            req.resolved_properties = Some(resolve_properties(
                &ctx,
                &self.event_properties,
                properties::Type::Event,
                props,
            )?);
        }

        if let Some(props) = &req.user_properties {
            req.resolved_user_properties = Some(resolve_properties(
                &ctx,
                &self.user_properties,
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

        let md_event = block_on(self.events.get_or_create(
            ctx.organization_id.unwrap(),
            ctx.project_id.unwrap(),
            event_req,
        ))?;

        let rec_id = block_on(
            self.events
                .generate_record_id(ctx.organization_id.unwrap(), ctx.project_id.unwrap()),
        )?;
        let event = Event {
            record_id: rec_id,
            event: md_event,
        };

        req.resolved_event = Some(event);

        for processor in &mut self.processors {
            req = processor.process(&ctx, req)?;
        }

        for dest in &mut self.destinations {
            dest.send(&ctx, req.clone())?;
        }

        Ok(())
    }
}

impl Executor<Identify> {
    pub fn new(
        processors: Vec<Arc<dyn Processor<Identify>>>,
        destinations: Vec<Arc<dyn Destination<Identify>>>,
        event_properties: Arc<dyn properties::Provider>,
        user_properties: Arc<dyn properties::Provider>,
        events: Arc<dyn events::Provider>,
        projects: Arc<dyn projects::Provider>,
    ) -> Self {
        Self {
            processors,
            destinations,
            event_properties,
            user_properties,
            events,
            projects,
        }
    }

    pub fn execute(&mut self, ctx: &RequestContext, mut req: Identify) -> Result<()> {
        let mut ctx = ctx.to_owned();
        let project = block_on(self.projects.get_by_token(ctx.token.as_str()))?;
        let mut ctx = ctx.to_owned();
        ctx.organization_id = Some(project.organization_id);
        ctx.project_id = Some(project.id);

        for processor in &mut self.processors {
            req = processor.process(&ctx, req)?;
        }

        for dest in &mut self.destinations {
            dest.send(&ctx, req.clone())?;
        }
        Ok(())
    }
}

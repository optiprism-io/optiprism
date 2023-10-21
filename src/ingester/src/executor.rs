use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use chrono::Utc;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use futures::executor::block_on;
use metadata::events;
use metadata::events::CreateEventRequest;
use metadata::properties;
use metadata::properties::CreatePropertyRequest;
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
    name: String,
    val: &PropValue,
) -> Result<Property> {
    let dt = match val {
        PropValue::Date(_) => DataType::Date64,
        PropValue::String(_) => DataType::Utf8,
        PropValue::Number(_) => DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
        PropValue::Bool(_) => DataType::Boolean,
    };

    let (is_dictionary, dictionary_type) = if let PropValue::String(_) = val {
        (true, Some(DataType::UInt64))
    } else {
        (false, None)
    };
    let req = CreatePropertyRequest {
        created_by: 1,
        tags: None,
        name: name.clone(),
        description: None,
        display_name: None,
        typ: dt,
        status: Status::Enabled,
        is_system: false,
        nullable: false,
        is_array: false,
        is_dictionary,
        dictionary_type,
    };

    let prop = block_on(properties.get_or_create(ctx.organization_id, ctx.project_id, req))?;
    Ok(Property {
        id: prop.id,
        name,
        value: val.to_owned().into(),
    })
}

fn resolve_properties(
    ctx: &RequestContext,
    properties: &Arc<dyn properties::Provider>,
    props: &HashMap<String, PropValue>,
) -> Result<Vec<Property>> {
    let resolved_props = props
        .iter()
        .map(|(k, v)| resolve_property(&ctx, properties, k.to_owned(), v))
        .collect::<Result<Vec<Property>>>()?;
    Ok(resolved_props)
}

pub struct Executor<T> {
    processors: Vec<Arc<dyn Processor<T>>>,
    destinations: Vec<Arc<dyn Destination<T>>>,
    event_properties: Arc<dyn properties::Provider>,
    user_properties: Arc<dyn properties::Provider>,
    events: Arc<dyn events::Provider>,
}

impl Executor<Track> {
    pub fn new(
        processors: Vec<Arc<dyn Processor<Track>>>,
        destinations: Vec<Arc<dyn Destination<Track>>>,
        event_properties: Arc<dyn properties::Provider>,
        user_properties: Arc<dyn properties::Provider>,
        events: Arc<dyn events::Provider>,
    ) -> Self {
        Self {
            processors,
            destinations,
            event_properties,
            user_properties,
            events,
        }
    }

    pub fn execute(&mut self, token: String, mut req: Track) -> Result<()> {
        let ctx = RequestContext {
            project_id: 1,
            organization_id: 1,
        };

        if let Some(props) = &req.properties {
            req.resolved_properties =
                Some(resolve_properties(&ctx, &self.event_properties, props)?);
        }

        if let Some(props) = &req.user_properties {
            req.resolved_user_properties =
                Some(resolve_properties(&ctx, &self.user_properties, props)?);
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
                .map(|props| props.iter().map(|p| p.id).collect::<Vec<u64>>()),
            custom_properties: None,
        };

        let md_event = block_on(self.events.get_or_create(
            ctx.organization_id,
            ctx.project_id,
            event_req,
        ))?;

        let event = Event {
            id: md_event.id,
            name: req.event.to_owned(),
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
    ) -> Self {
        Self {
            processors,
            destinations,
            event_properties,
            user_properties,
            events,
        }
    }

    pub fn execute(&mut self, token: String, mut req: Identify) -> Result<()> {
        let ctx = RequestContext {
            project_id: 1,
            organization_id: 1,
        };

        for processor in &mut self.processors {
            req = processor.process(&ctx, req)?;
        }

        for dest in &mut self.destinations {
            dest.send(&ctx, req.clone())?;
        }
        Ok(())
    }
}

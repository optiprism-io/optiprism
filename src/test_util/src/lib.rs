use std::sync::Arc;

use arrow::datatypes::Field;
use arrow::datatypes::TimeUnit;
use common::types::{DType, TIME_UNIT};
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use metadata::events;
use metadata::events::Event;
use metadata::properties;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::DictionaryType;
use metadata::properties::Property;
use metadata::properties::Type;
use metadata::MetadataProvider;
use store::db::OptiDBImpl;

pub fn create_event(
    md: &Arc<MetadataProvider>,
    org_id: u64,
    proj_id: u64,
    name: String,
) -> anyhow::Result<Event> {
    Ok(md
        .events
        .get_or_create(org_id, proj_id, events::CreateEventRequest {
            created_by: 0,
            tags: None,
            name,
            display_name: None,
            description: None,
            status: events::Status::Enabled,
            is_system: false,
            properties: None,
            custom_properties: None,
        })?)
}

pub struct CreatePropertyMainRequest {
    pub name: String,
    pub typ: Type,
    pub data_type: DType,
    pub nullable: bool,
    pub dict: Option<DictionaryType>,
}

pub fn create_property(
    md: &Arc<MetadataProvider>,
    org_id: u64,
    proj_id: u64,
    main_req: CreatePropertyMainRequest,
    db: &Arc<OptiDBImpl>,
) -> anyhow::Result<Property> {
    let req = CreatePropertyRequest {
        created_by: 0,
        tags: None,
        name: main_req.name.clone(),
        description: None,
        display_name: None,
        typ: main_req.typ,
        data_type: main_req.data_type.clone(),
        status: properties::Status::Enabled,
        is_system: false,
        nullable: main_req.nullable,
        is_array: false,
        is_dictionary: main_req.dict.is_some(),
        dictionary_type: main_req.dict.clone(),
    };

    let prop = md.event_properties.get_or_create(org_id, proj_id, req)?;

    db.add_field("events", prop.column_name().as_str(), main_req.data_type, main_req.nullable)?;

    Ok(prop)
}

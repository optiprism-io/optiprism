use std::env::temp_dir;
use std::sync::Arc;
use uuid::Uuid;
use common::types::DType;
use store::db::{OptiDBImpl, Options, TableOptions};
use crate::events::Event;
use crate::{events, MetadataProvider, properties};
use crate::properties::{CreatePropertyRequest, DictionaryType, Property, Type};

pub fn init_db() -> anyhow::Result<(Arc<MetadataProvider>, Arc<OptiDBImpl>)> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(crate::rocksdb::new(path.join("md"))?);

    let db = Arc::new(OptiDBImpl::open(path.join("db"),Options {})?);
    let opts = TableOptions::test();
    db.create_table("events", opts).unwrap();
    Ok((Arc::new(MetadataProvider::try_new(store, db.clone())?),db))
}

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
) -> anyhow::Result<Property> {
    let req = CreatePropertyRequest {
        created_by: 0,
        tags: None,
        name: main_req.name.clone(),
        description: None,
        display_name: None,
        typ: main_req.typ.clone(),
        data_type: main_req.data_type.clone(),
        status: properties::Status::Enabled,
        is_system: false,
        nullable: main_req.nullable,
        is_array: false,
        is_dictionary: main_req.dict.is_some(),
        dictionary_type: main_req.dict.clone(),
    };

    let prop = match main_req.typ {
        Type::System => md.system_properties.get_or_create(org_id, proj_id, req)?,
        Type::Event => md.event_properties.get_or_create(org_id, proj_id, req)?,
        Type::User => md.user_properties.get_or_create(org_id, proj_id, req)?
    };

    Ok(prop)
}

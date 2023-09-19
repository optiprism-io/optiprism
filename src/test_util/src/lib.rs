use std::sync::Arc;

use arrow::datatypes::DataType;
use metadata::database::Column;
use metadata::events;
use metadata::events::Event;
use metadata::properties;
use metadata::properties::provider_impl::Namespace;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::Property;
use metadata::MetadataProvider;

pub async fn create_event(
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
        })
        .await?)
}

pub struct CreatePropertyMainRequest {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub dict: Option<DataType>,
}

pub async fn create_property(
    md: &Arc<MetadataProvider>,
    ns: Namespace,
    org_id: u64,
    proj_id: u64,
    main_req: CreatePropertyMainRequest,
    cols: &mut Vec<Column>,
) -> anyhow::Result<Property> {
    let req = CreatePropertyRequest {
        created_by: 0,
        tags: None,
        name: main_req.name.clone(),
        description: None,
        display_name: None,
        typ: main_req.data_type.clone(),
        status: properties::Status::Enabled,
        is_system: false,
        nullable: main_req.nullable,
        is_array: false,
        is_dictionary: main_req.dict.is_some(),
        dictionary_type: main_req.dict.clone(),
    };

    let prop = match ns {
        Namespace::Event => {
            md.event_properties
                .get_or_create(org_id, proj_id, req)
                .await?
        }
        Namespace::User => {
            md.user_properties
                .get_or_create(org_id, proj_id, req)
                .await?
        }
    };

    cols.push(Column::new(
        prop.column_name(ns),
        main_req.data_type,
        main_req.nullable,
        main_req.dict,
    ));

    Ok(prop)
}

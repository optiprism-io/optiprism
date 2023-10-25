use std::sync::Arc;

use arrow::datatypes::TimeUnit;
use common::types::TIME_UNIT;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use metadata::database::Column;
use metadata::events;
use metadata::events::Event;
use metadata::properties;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::DataType;
use metadata::properties::DictionaryType;
use metadata::properties::Property;
use metadata::properties::Type;
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
    pub typ: Type,
    pub data_type: DataType,
    pub nullable: bool,
    pub dict: Option<DictionaryType>,
}

pub async fn create_property(
    md: &Arc<MetadataProvider>,
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
        typ: main_req.typ,
        data_type: main_req.data_type.clone(),
        status: properties::Status::Enabled,
        is_system: false,
        nullable: main_req.nullable,
        is_array: false,
        is_dictionary: main_req.dict.is_some(),
        dictionary_type: main_req.dict.clone(),
    };

    let prop = md
        .event_properties
        .get_or_create(org_id, proj_id, req)
        .await?;

    let dt = match main_req.data_type {
        DataType::String => arrow::datatypes::DataType::Utf8,
        DataType::Int8 => arrow::datatypes::DataType::Int8,
        DataType::Int16 => arrow::datatypes::DataType::Int16,
        DataType::Int32 => arrow::datatypes::DataType::Int32,
        DataType::Int64 => arrow::datatypes::DataType::Int64,
        DataType::UInt8 => arrow::datatypes::DataType::UInt8,
        DataType::UInt16 => arrow::datatypes::DataType::UInt16,
        DataType::UInt32 => arrow::datatypes::DataType::UInt32,
        DataType::UInt64 => arrow::datatypes::DataType::UInt64,
        DataType::Float64 => arrow::datatypes::DataType::Float64,
        DataType::Decimal => {
            arrow::datatypes::DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE)
        }
        DataType::Boolean => arrow::datatypes::DataType::Boolean,
        DataType::Timestamp => arrow::datatypes::DataType::Timestamp(TIME_UNIT, None),
    };

    cols.push(Column::new(
        prop.column_name(),
        dt,
        main_req.nullable,
        main_req.dict,
    ));

    Ok(prop)
}

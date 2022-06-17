use std::sync::Arc;
use arrow::datatypes::{DataType, Schema, TimeUnit};
use arrow::datatypes::DataType::UInt8;
use common::{DECIMAL_PRECISION, DECIMAL_SCALE};
use metadata::database::{Column, Table, TableType};
use metadata::{events, Metadata, properties};
use metadata::events::Event as MDEvent;
use metadata::properties::{CreatePropertyRequest, Property};
use metadata::properties::provider::Namespace;
use query::event_fields;
use crate::error::{Result, Error};
use crate::store::events::Event;
use enum_iterator::all;

async fn create_event(md: &Arc<Metadata>, org_id: u64, proj_id: u64, name: String) -> Result<MDEvent> {
    Ok(
        md.events
            .get_or_create(
                org_id,
                proj_id,
                events::CreateEventRequest {
                    created_by: 0,
                    tags: None,
                    name,
                    display_name: None,
                    description: None,
                    status: events::Status::Enabled,
                    properties: None,
                    custom_properties: None,
                    is_system: false
                },
            )
            .await?
    )
}

async fn create_property(
    md: &Arc<Metadata>,
    ns: Namespace,
    org_id: u64,
    proj_id: u64,
    name: String,
    data_type: DataType,
    nullable: bool,
    dict: Option<DataType>,
    cols: &mut Vec<Column>,
) -> Result<Property> {
    let req = CreatePropertyRequest {
        created_by: 0,
        tags: None,
        name,
        description: None,
        display_name: None,
        typ: data_type.clone(),
        status: properties::Status::Enabled,
        is_system: false,
        nullable,
        is_array: false,
        is_dictionary: dict.is_some(),
        dictionary_type: dict.clone(),
    };

    let prop = match ns {
        Namespace::Event => md.event_properties.get_or_create(org_id, proj_id, req).await?,
        Namespace::User => md.user_properties.get_or_create(org_id, proj_id, req).await?,
    };

    cols.push(Column::new(prop.column_name(ns), data_type, nullable, dict));

    Ok(prop)
}

pub async fn create_entities(org_id: u64, proj_id: u64, md: &Arc<Metadata>) -> Result<Schema> {
    let mut cols: Vec<Column> = Vec::new();

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        "User ID".to_string(),
        DataType::UInt64,
        false,
        None,
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        "Created At".to_string(),
        DataType::Timestamp(TimeUnit::Second, None),
        false,
        None,
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        "Event".to_string(),
        DataType::Utf8,
        false,
        Some(DataType::UInt64),
        &mut cols,
    ).await?;

    // create event props
    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        "Product Name".to_string(),
        DataType::Utf8,
        true,
        Some(DataType::UInt16),
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        "Product Category".to_string(),
        DataType::Utf8,
        true,
        Some(DataType::UInt16),
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        "Product Subcategory".to_string(),
        DataType::Utf8,
        true,
        Some(DataType::UInt16),
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        "Product Brand".to_string(),
        DataType::Utf8,
        true,
        Some(DataType::UInt16),
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        "Product Price".to_string(),
        DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
        true,
        None,
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        "Product Discount Price".to_string(),
        DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
        true,
        None,
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        "Spent Total".to_string(),
        DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
        true,
        None,
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        "Products Bought".to_string(),
        UInt8,
        true,
        None,
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        "Cart Items Number".to_string(),
        UInt8,
        true,
        None,
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        "Cart Amount".to_string(),
        DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
        true,
        None,
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        "Revenue".to_string(),
        DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE),
        true,
        None,
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        "Country".to_string(),
        DataType::Utf8,
        true,
        Some(DataType::UInt16),
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        "City".to_string(),
        DataType::Utf8,
        true,
        Some(DataType::UInt16),
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        "Device".to_string(),
        DataType::Utf8,
        true,
        Some(DataType::UInt16),
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        "Device Category".to_string(),
        DataType::Utf8,
        true,
        Some(DataType::UInt16),
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        "Os".to_string(),
        DataType::Utf8,
        true,
        Some(DataType::UInt16),
        &mut cols,
    ).await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        "Os Version".to_string(),
        DataType::Utf8,
        true,
        Some(DataType::UInt16),
        &mut cols,
    ).await?;


    for event in all::<Event>() {
        create_event(md, org_id, proj_id, event.to_string()).await?;
    }

    let table = Table {
        typ: TableType::Events(org_id, proj_id),
        columns: cols,
    };
    match md.database
        .create_table(table.clone())
        .await {
        Ok(_) | Err(metadata::error::Error::KeyAlreadyExists) => {}
        Err(err) => return Err(err.into()),
    };

    Ok(table.arrow_schema())
}
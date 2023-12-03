use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use enum_iterator::all;
use metadata::database::Column;
use metadata::database::CreateTableRequest;
use metadata::database::Table;
use metadata::database::TableRef;
use metadata::error::MetadataError;
use metadata::properties::DataType;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::MetadataProvider;
use store::db::OptiDBImpl;
use test_util::create_event;
use test_util::create_property;
use test_util::CreatePropertyMainRequest;

use crate::error::Result;
use crate::store::events::Event;

pub fn create_entities(
    org_id: u64,
    proj_id: u64,
    md: &Arc<MetadataProvider>,
    db: Arc<OptiDBImpl>,
) -> Result<Schema> {
    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "User ID".to_string(),
            typ: Type::Event,
            data_type: DataType::Int64,
            nullable: false,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Created At".to_string(),
            typ: Type::Event,
            data_type: DataType::Timestamp,
            nullable: false,
            dict: None,
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Event".to_string(),
            typ: Type::Event,
            data_type: DataType::String,
            nullable: false,
            dict: Some(DictionaryType::UInt64),
        },
        &mut cols,
    )?;

    // create event props
    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Name".to_string(),
            typ: Type::Event,
            data_type: DataType::String,
            nullable: true,
            dict: Some(DictionaryType::UInt16),
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Category".to_string(),
            typ: Type::Event,
            data_type: DataType::String,
            nullable: true,
            dict: Some(DictionaryType::UInt16),
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Subcategory".to_string(),
            typ: Type::Event,
            data_type: DataType::String,
            nullable: true,
            dict: Some(DictionaryType::UInt16),
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Brand".to_string(),
            typ: Type::Event,
            data_type: DataType::String,
            nullable: true,
            dict: Some(DictionaryType::UInt16),
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Price".to_string(),
            typ: Type::Event,
            data_type: DataType::Decimal,
            nullable: true,
            dict: None,
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Discount Price".to_string(),
            typ: Type::Event,
            data_type: DataType::Decimal,
            nullable: true,
            dict: None,
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Spent Total".to_string(),
            typ: Type::Event,
            data_type: DataType::Decimal,
            nullable: true,
            dict: None,
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Products Bought".to_string(),
            typ: Type::Event,
            data_type: DataType::UInt8,
            nullable: true,
            dict: None,
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Cart Items Number".to_string(),
            typ: Type::Event,
            data_type: DataType::UInt8,
            nullable: true,
            dict: None,
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Cart Amount".to_string(),
            typ: Type::Event,
            data_type: DataType::Decimal,
            nullable: true,
            dict: None,
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Revenue".to_string(),
            typ: Type::Event,
            data_type: DataType::Decimal,
            nullable: true,
            dict: None,
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Country".to_string(),
            typ: Type::User,
            data_type: DataType::String,
            nullable: true,
            dict: Some(DictionaryType::UInt16),
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "City".to_string(),
            typ: Type::User,
            data_type: DataType::String,
            nullable: true,
            dict: Some(DictionaryType::UInt16),
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Device".to_string(),
            typ: Type::User,
            data_type: DataType::String,
            nullable: true,
            dict: Some(DictionaryType::UInt16),
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Device Category".to_string(),
            typ: Type::User,
            data_type: DataType::String,
            nullable: true,
            dict: Some(DictionaryType::UInt16),
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Os".to_string(),
            typ: Type::User,
            data_type: DataType::String,
            nullable: true,
            dict: Some(DictionaryType::UInt16),
        },
        &mut cols,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Os Version".to_string(),
            typ: Type::User,
            data_type: DataType::String,
            nullable: true,
            dict: Some(DictionaryType::UInt16),
        },
        &mut cols,
    )?;

    for event in all::<Event>() {
        create_event(md, org_id, proj_id, event.to_string())?;
    }

    let table = CreateTableRequest {
        typ: TableRef::Events(org_id, proj_id),
        columns: cols.clone(),
    };

    match md.database.create_table(table.clone()) {
        Ok(_) | Err(MetadataError::AlreadyExists(_)) => {}
        Err(err) => return Err(err.into()),
    };
    let table = Table {
        id: 1,
        typ: TableRef::Events(org_id, proj_id),
        columns: cols,
    };
    Ok(table.arrow_schema())
}

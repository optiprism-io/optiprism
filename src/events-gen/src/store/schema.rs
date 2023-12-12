use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use enum_iterator::all;
use common::types::{COLUMN_EVENT, COLUMN_PROJECT_ID, COLUMN_CREATED_AT, COLUMN_USER_ID, DType};
use metadata::error::MetadataError;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::MetadataProvider;
use store::db::OptiDBImpl;
use test_util::create_event;
use test_util::create_property;
use test_util::CreatePropertyMainRequest;

use crate::error::Result;
use crate::store::events::Event;

pub fn create_properties(
    org_id: u64,
    proj_id: u64,
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
) -> Result<()> {
    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: COLUMN_PROJECT_ID.to_string(),
            typ: Type::Event,
            data_type: DType::Int64,
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
            name: COLUMN_USER_ID.to_string(),
            typ: Type::Event,
            data_type: DType::Int64,
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
            name: COLUMN_CREATED_AT.to_string(),
            typ: Type::Event,
            data_type: DType::Timestamp,
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
            name: COLUMN_EVENT.to_string(),
            typ: Type::Event,
            data_type: DType::String,
            nullable: false,
            dict: Some(DictionaryType::Int64),
        },
        &db,
    )?;

    // create event props
    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Name".to_string(),
            typ: Type::Event,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int16),
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Category".to_string(),
            typ: Type::Event,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int16),
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Subcategory".to_string(),
            typ: Type::Event,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int16),
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Brand".to_string(),
            typ: Type::Event,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int16),
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Price".to_string(),
            typ: Type::Event,
            data_type: DType::Decimal,
            nullable: true,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Discount Price".to_string(),
            typ: Type::Event,
            data_type: DType::Decimal,
            nullable: true,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Spent Total".to_string(),
            typ: Type::Event,
            data_type: DType::Decimal,
            nullable: true,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Products Bought".to_string(),
            typ: Type::Event,
            data_type: DType::Int8,
            nullable: true,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Cart Items Number".to_string(),
            typ: Type::Event,
            data_type: DType::Int8,
            nullable: true,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Cart Amount".to_string(),
            typ: Type::Event,
            data_type: DType::Decimal,
            nullable: true,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Revenue".to_string(),
            typ: Type::Event,
            data_type: DType::Decimal,
            nullable: true,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Country".to_string(),
            typ: Type::User,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int16),
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "City".to_string(),
            typ: Type::User,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int16),
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Device".to_string(),
            typ: Type::User,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int16),
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Device Category".to_string(),
            typ: Type::User,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int16),
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Os".to_string(),
            typ: Type::User,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int16),
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Os Version".to_string(),
            typ: Type::User,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int16),
        },
        &db,
    )?;

    for event in all::<Event>() {
        create_event(md, org_id, proj_id, event.to_string())?;
    }

    Ok(())
}

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use common::types::DType;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use common::types::COLUMN_USER_ID;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use enum_iterator::all;
use metadata::error::MetadataError;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::test_util::create_event;
use metadata::test_util::create_property;
use metadata::test_util::CreatePropertyMainRequest;
use metadata::MetadataProvider;
use store::db::OptiDBImpl;

use crate::error::Result;
use crate::store::events::Event;

pub fn create_properties(
    org_id: u64,
    proj_id: u64,
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
) -> Result<()> {
    // create event props
    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Product Name".to_string(),
        typ: Type::Event,
        data_type: DType::String,
        nullable: true,
        dict: Some(DictionaryType::Int16),
    })?;

    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Product Category".to_string(),
        typ: Type::Event,
        data_type: DType::String,
        nullable: true,
        dict: Some(DictionaryType::Int16),
    })?;

    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Product Subcategory".to_string(),
        typ: Type::Event,
        data_type: DType::String,
        nullable: true,
        dict: Some(DictionaryType::Int16),
    })?;

    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Product Brand".to_string(),
        typ: Type::Event,
        data_type: DType::String,
        nullable: true,
        dict: Some(DictionaryType::Int16),
    })?;

    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Product Price".to_string(),
        typ: Type::Event,
        data_type: DType::Decimal,
        nullable: true,
        dict: None,
    })?;

    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Product Discount Price".to_string(),
        typ: Type::Event,
        data_type: DType::Decimal,
        nullable: true,
        dict: None,
    })?;

    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Revenue".to_string(),
        typ: Type::Event,
        data_type: DType::Decimal,
        nullable: true,
        dict: None,
    })?;

    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Spent Total".to_string(),
        typ: Type::User,
        data_type: DType::Decimal,
        nullable: true,
        dict: None,
    })?;

    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Products Bought".to_string(),
        typ: Type::User,
        data_type: DType::Int8,
        nullable: true,
        dict: None,
    })?;

    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Cart Items Number".to_string(),
        typ: Type::User,
        data_type: DType::Int8,
        nullable: true,
        dict: None,
    })?;

    create_property(md, org_id, proj_id, CreatePropertyMainRequest {
        name: "Cart Amount".to_string(),
        typ: Type::User,
        data_type: DType::Decimal,
        nullable: true,
        dict: None,
    })?;

    for event in all::<Event>() {
        create_event(md, org_id, proj_id, event.to_string())?;
    }

    Ok(())
}

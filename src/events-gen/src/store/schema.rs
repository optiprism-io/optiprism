use std::sync::Arc;

use common::types::DType;
use enum_iterator::all;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::test_util::create_event;
use metadata::test_util::create_property;
use metadata::test_util::CreatePropertyMainRequest;
use metadata::MetadataProvider;
use storage::db::OptiDBImpl;

use crate::error::Result;
use crate::store::events::Event;

pub fn create_properties(
    proj_id: u64,
    md: &Arc<MetadataProvider>,
    _db: &Arc<OptiDBImpl>,
) -> Result<()> {
    // create event props
    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Product Name".to_string(),
        typ: Type::Event,
        data_type: DType::String,
        nullable: true,
        dict: Some(DictionaryType::Int16),
    })?;

    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Product Category".to_string(),
        typ: Type::Event,
        data_type: DType::String,
        nullable: true,
        dict: Some(DictionaryType::Int16),
    })?;

    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Product Subcategory".to_string(),
        typ: Type::Event,
        data_type: DType::String,
        nullable: true,
        dict: Some(DictionaryType::Int16),
    })?;

    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Product Brand".to_string(),
        typ: Type::Event,
        data_type: DType::String,
        nullable: true,
        dict: Some(DictionaryType::Int16),
    })?;

    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Product Price".to_string(),
        typ: Type::Event,
        data_type: DType::Decimal,
        nullable: true,
        dict: None,
    })?;

    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Product Discount Price".to_string(),
        typ: Type::Event,
        data_type: DType::Decimal,
        nullable: true,
        dict: None,
    })?;

    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Revenue".to_string(),
        typ: Type::Event,
        data_type: DType::Decimal,
        nullable: true,
        dict: None,
    })?;

    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Spent Total".to_string(),
        typ: Type::User,
        data_type: DType::Decimal,
        nullable: true,
        dict: None,
    })?;

    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Products Bought".to_string(),
        typ: Type::User,
        data_type: DType::Int8,
        nullable: true,
        dict: None,
    })?;

    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Cart Items Number".to_string(),
        typ: Type::User,
        data_type: DType::Int8,
        nullable: true,
        dict: None,
    })?;

    create_property(md, proj_id, CreatePropertyMainRequest {
        name: "Cart Amount".to_string(),
        typ: Type::User,
        data_type: DType::Decimal,
        nullable: true,
        dict: None,
    })?;

    for event in all::<Event>() {
        create_event(md, proj_id, event.to_string())?;
    }

    Ok(())
}

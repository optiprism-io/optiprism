use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use enum_iterator::all;
use metadata::database::Column;
use metadata::database::Table;
use metadata::database::TableRef;
use metadata::error::DatabaseError;
use metadata::properties::provider_impl::Namespace;
use metadata::MetadataProvider;
use test_util::create_event;
use test_util::create_property;
use test_util::CreatePropertyMainRequest;

use crate::error::Result;
use crate::store::events::Event;

pub async fn create_entities(
    org_id: u64,
    proj_id: u64,
    md: &Arc<MetadataProvider>,
) -> Result<Schema> {
    let mut cols: Vec<Column> = Vec::new();

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "User ID".to_string(),
            data_type: DataType::Int64,
            nullable: false,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Created At".to_string(),
            data_type: DataType::Timestamp(TimeUnit::Nanosecond, None),
            nullable: false,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Event".to_string(),
            data_type: DataType::Utf8,
            nullable: false,
            dict: Some(DataType::UInt64),
        },
        &mut cols,
    )
    .await?;

    // create event props
    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Name".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            dict: Some(DataType::UInt16),
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Category".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            dict: Some(DataType::UInt16),
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Subcategory".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            dict: Some(DataType::UInt16),
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Brand".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            dict: Some(DataType::UInt16),
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Price".to_string(),
            data_type: DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            nullable: true,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Product Discount Price".to_string(),
            data_type: DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            nullable: true,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Spent Total".to_string(),
            data_type: DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            nullable: true,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Products Bought".to_string(),
            data_type: DataType::UInt8,
            nullable: true,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Cart Items Number".to_string(),
            data_type: DataType::UInt8,
            nullable: true,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Cart Amount".to_string(),
            data_type: DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            nullable: true,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Revenue".to_string(),
            data_type: DataType::Decimal128(DECIMAL_PRECISION, DECIMAL_SCALE),
            nullable: true,
            dict: None,
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Country".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            dict: Some(DataType::UInt16),
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "City".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            dict: Some(DataType::UInt16),
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Device".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            dict: Some(DataType::UInt16),
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Device Category".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            dict: Some(DataType::UInt16),
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Os".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            dict: Some(DataType::UInt16),
        },
        &mut cols,
    )
    .await?;

    create_property(
        md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: "Os Version".to_string(),
            data_type: DataType::Utf8,
            nullable: true,
            dict: Some(DataType::UInt16),
        },
        &mut cols,
    )
    .await?;

    for event in all::<Event>() {
        create_event(md, org_id, proj_id, event.to_string()).await?;
    }

    let table = Table {
        typ: TableRef::Events(org_id, proj_id),
        columns: cols,
    };

    match md.database.create_table(table.clone()).await {
        Ok(_)
        | Err(metadata::error::MetadataError::Database(DatabaseError::TableAlreadyExists(_))) => {}
        Err(err) => return Err(err.into()),
    };

    Ok(table.arrow_schema())
}

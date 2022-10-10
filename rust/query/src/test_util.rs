use std::env::temp_dir;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::provider_as_source;
use datafusion::logical_plan::LogicalPlan;
use datafusion::prelude::CsvReadOptions;
use datafusion_expr::logical_plan::builder::UNNAMED_TABLE;
use datafusion_expr::LogicalPlanBuilder;
use uuid::Uuid;

use metadata::database::{Column, Table, TableRef};
use metadata::properties::provider::Namespace;
use metadata::properties::{CreatePropertyRequest, Property};
use metadata::store::Store;
use metadata::{database, events, properties, MetadataProvider};

use crate::error::Result;
use crate::event_fields;

pub async fn events_provider(
    db: Arc<database::Provider>,
    org_id: u64,
    proj_id: u64,
) -> Result<LogicalPlan> {
    let table = db.get_table(TableRef::Events(org_id, proj_id)).await?;
    let schema = table.arrow_schema();

    let options = CsvReadOptions::new();
    let table_path = ListingTableUrl::parse("../tests/events.csv")?;
    let target_partitions = 1;
    let listing_options = options.to_listing_options(target_partitions);

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(Arc::new(schema));
    let provider = ListingTable::try_new(config)?;
    Ok(
        LogicalPlanBuilder::scan(UNNAMED_TABLE, provider_as_source(Arc::new(provider)), None)?
            .build()?,
    )
}

pub fn empty_provider() -> Result<LogicalPlan> {
    Ok(LogicalPlanBuilder::empty(false).build()?)
}
pub async fn create_property(
    md: &Arc<MetadataProvider>,
    ns: Namespace,
    org_id: u64,
    proj_id: u64,
    req: CreatePropertyRequest,
) -> Result<Property> {
    let prop = match ns {
        Namespace::Event => md.event_properties.create(org_id, proj_id, req).await?,
        Namespace::User => md.user_properties.create(org_id, proj_id, req).await?,
    };

    md.database
        .add_column(
            TableRef::Events(org_id, proj_id),
            Column::new(
                prop.column_name(ns),
                prop.typ.clone(),
                prop.nullable,
                prop.dictionary_type.clone(),
            ),
        )
        .await?;

    Ok(prop)
}

pub async fn create_entities(md: Arc<MetadataProvider>, org_id: u64, proj_id: u64) -> Result<()> {
    md.database
        .create_table(Table {
            typ: TableRef::Events(org_id, proj_id),
            columns: vec![],
        })
        .await?;

    md.database
        .add_column(
            TableRef::Events(org_id, proj_id),
            Column::new(
                event_fields::USER_ID.to_string(),
                DataType::UInt64,
                false,
                None,
            ),
        )
        .await?;
    md.database
        .add_column(
            TableRef::Events(org_id, proj_id),
            Column::new(
                event_fields::CREATED_AT.to_string(),
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                false,
                None,
            ),
        )
        .await?;
    md.database
        .add_column(
            TableRef::Events(org_id, proj_id),
            Column::new(
                event_fields::EVENT.to_string(),
                DataType::UInt16,
                false,
                None,
            ),
        )
        .await?;

    // create user props

    let country_prop = create_property(
        &md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "Country".to_string(),
            description: None,
            display_name: None,
            typ: DataType::Utf8,
            status: properties::Status::Enabled,
            is_system: false,
            nullable: false,
            is_array: false,
            is_dictionary: false,
            dictionary_type: Some(DataType::UInt8),
        },
    )
    .await?;

    md.dictionaries
        .get_key_or_create(
            org_id,
            proj_id,
            country_prop.column_name(Namespace::User).as_str(),
            "spain",
        )
        .await?;
    md.dictionaries
        .get_key_or_create(
            org_id,
            proj_id,
            country_prop.column_name(Namespace::User).as_str(),
            "german",
        )
        .await?;
    create_property(
        &md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "Device".to_string(),
            description: None,
            display_name: None,
            typ: DataType::Utf8,
            status: properties::Status::Enabled,
            nullable: true,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
            is_system: false,
        },
    )
    .await?;

    create_property(
        &md,
        Namespace::User,
        org_id,
        proj_id,
        CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "Is Premium".to_string(),
            description: None,
            display_name: None,
            typ: DataType::Boolean,
            status: properties::Status::Enabled,
            is_system: false,
            nullable: false,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
        },
    )
    .await?;

    // create events
    md.events
        .create(
            org_id,
            proj_id,
            events::CreateEventRequest {
                created_by: 0,
                tags: None,
                name: "View Product".to_string(),
                display_name: None,
                description: None,
                status: events::Status::Enabled,
                properties: None,
                custom_properties: None,
                is_system: false,
            },
        )
        .await?;

    md.events
        .create(
            org_id,
            proj_id,
            events::CreateEventRequest {
                created_by: 0,
                tags: None,
                name: "Buy Product".to_string(),
                display_name: None,
                description: None,
                status: events::Status::Enabled,
                properties: None,
                custom_properties: None,
                is_system: false,
            },
        )
        .await?;

    // create event props
    create_property(
        &md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "Product Name".to_string(),
            description: None,
            display_name: None,
            typ: DataType::Utf8,
            status: properties::Status::Enabled,
            nullable: false,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
            is_system: false,
        },
    )
    .await?;

    create_property(
        &md,
        Namespace::Event,
        org_id,
        proj_id,
        CreatePropertyRequest {
            created_by: 0,
            tags: None,
            name: "Revenue".to_string(),
            description: None,
            display_name: None,
            typ: DataType::Float64,
            status: properties::Status::Enabled,
            nullable: true,
            is_array: false,
            is_dictionary: false,
            dictionary_type: None,
            is_system: false,
        },
    )
    .await?;

    Ok(())
}

pub fn create_md() -> Result<Arc<MetadataProvider>> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    Ok(Arc::new(MetadataProvider::try_new(store)?))
}

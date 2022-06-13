#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::sync::Arc;
    use axum::{AddExtensionLayer, Router, Server};
    use uuid::Uuid;
    use metadata::{Metadata, Store};
    use platform::error::Result;
    use platform::{Context, EventSegmentationProvider};
    use query::{event_fields, QueryProvider};
    use platform::http::event_segmentation;
    use std::{net::SocketAddr};
    use std::time::Duration;
    use tokio::time::sleep;

    use std::borrow::BorrowMut;

    use chrono::{DateTime, Utc};
    use datafusion::arrow::array::{
        Float64Array, Int32Array, Int8Array, StringArray, TimestampMicrosecondArray, UInt16Array,
        UInt64Array,
    };

    use arrow::datatypes::{DataType as DFDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::print_batches;
    use datafusion::datasource::object_store::local::LocalFileSystem;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::planner::DefaultPhysicalPlanner;
    use datafusion::physical_plan::{aggregates, collect, PhysicalPlanner};
    use datafusion::prelude::{CsvReadOptions, ExecutionConfig, ExecutionContext};

    use datafusion::execution::context::ExecutionContextState;
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::logical_plan::{LogicalPlan, TableScan};
    use metadata::database::{Column, Table, TableType};
    use metadata::properties::provider::Namespace;
    use metadata::properties::{CreatePropertyRequest, Property};
    use metadata::{database, events, properties};
    use rust_decimal::Decimal;
    use std::ops::Sub;
    use arrow::array::{Array, ArrayBuilder, ArrayRef, BooleanArray, BooleanBuilder, DecimalArray, DecimalBuilder, Float64Builder, Int16Array, Int16Builder, Int8BufferBuilder, Int8Builder, make_builder, StringBuilder, TimestampNanosecondArray, TimestampNanosecondBuilder, UInt64Builder, UInt8Builder};
    use arrow::buffer::MutableBuffer;
    use arrow::ipc::{TimestampBuilder, Utf8Builder};
    use axum::headers::{HeaderMap, HeaderValue};
    use axum::http::StatusCode;
    use reqwest::Client;
    use serde_json::Value;
    use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
    use datafusion::physical_plan::coalesce_batches::concat_batches;
    use datafusion::scalar::{ScalarValue as DFScalarValue, ScalarValue};
    use platform::event_segmentation::types::{Query, Analysis, Breakdown, ChartType, Event, EventFilter, EventSegmentation, EventType, PropertyType, PropValueOperation, QueryTime, TimeUnit, PartitionedAggregateFunction, AggregateFunction};
    use query::physical_plan::expressions;
    use query::reports::event_segmentation::logical_plan_builder::LogicalPlanBuilder;
    use query::reports::types::EventRef;
    use query::reports::event_segmentation::types as query_es_types;
    use query::reports::types as query_types;

    async fn create_property(
        md: &Arc<Metadata>,
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
                TableType::Events(org_id, proj_id),
                Column::new(prop.column_name(ns), prop.typ.clone(), prop.nullable, None),
            )
            .await?;

        Ok(prop)
    }

    async fn create_entities(md: Arc<Metadata>, org_id: u64, proj_id: u64) -> Result<()> {
        md.database
            .create_table(Table {
                typ: TableType::Events(org_id, proj_id),
                columns: vec![],
            })
            .await?;

        md.database
            .add_column(
                TableType::Events(org_id, proj_id),
                Column::new("user_id".to_string(), DFDataType::UInt64, false, None),
            )
            .await?;
        md.database
            .add_column(
                TableType::Events(org_id, proj_id),
                Column::new(
                    "created_at".to_string(),
                    DFDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                    false,
                    None,
                ),
            )
            .await?;
        md.database
            .add_column(
                TableType::Events(org_id, proj_id),
                Column::new("event".to_string(), DFDataType::UInt16, false, None),
            )
            .await?;

        // create user props
        create_property(
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
                typ: DFDataType::Utf8,
                status: properties::Status::Enabled,
                scope: properties::Scope::User,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
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
                name: "Device".to_string(),
                description: None,
                display_name: None,
                typ: DFDataType::Utf8,
                status: properties::Status::Enabled,
                scope: properties::Scope::User,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
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
                typ: DFDataType::Boolean,
                status: properties::Status::Enabled,
                scope: properties::Scope::User,
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
                    is_system: false,
                    properties: None,
                    custom_properties: None,
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
                    is_system: false,
                    properties: None,
                    custom_properties: None,
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
                typ: DFDataType::Utf8,
                status: properties::Status::Enabled,
                scope: properties::Scope::User,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
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
                typ: DFDataType::Float64,
                status: properties::Status::Enabled,
                scope: properties::Scope::User,
                nullable: false,
                is_array: false,
                is_dictionary: false,
                dictionary_type: None,
            },
        )
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_events() -> Result<()> {
        tokio::spawn(async {
            let mut path = temp_dir();
            path.push(format!("{}.db", Uuid::new_v4()));
            let store = Arc::new(Store::new(path));
            let md = Arc::new(Metadata::try_new(store).unwrap());
            create_entities(md.clone(), 0, 0).await.unwrap();

            let table = md.database.get_table(TableType::Events(0, 0)).await.unwrap();
            let schema = table.arrow_schema();
            let options = CsvReadOptions::new().schema(&schema);
            let path = "../tests/events.csv";
            let input = datafusion::logical_plan::LogicalPlanBuilder::scan_csv(
                Arc::new(LocalFileSystem {}),
                path,
                options,
                None,
                1,
            )
                .await.unwrap()
                .build().unwrap();

            let config = ListingTableConfig::new(Arc::new(LocalFileSystem {}), path)
                .with_schema(Arc::new(schema));

            let provider = ListingTable::try_new(config).unwrap();
            let query = QueryProvider::try_new(md, Arc::new(provider)).unwrap();
            let es_provider = Arc::new(EventSegmentationProvider::new(Arc::new(query)));
            let app = event_segmentation::attach_routes(Router::new(), es_provider);

            let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
            Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .unwrap();
        });

        sleep(Duration::from_millis(100)).await;

        let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);

        let es = EventSegmentation {
            time: QueryTime::Between {
                from,
                to,
            },
            group: "user_id".to_string(),
            interval_unit: TimeUnit::Minute,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![
                Event {
                    event_name: "View Product".to_string(),
                    event_type: EventType::Regular,
                    filters: Some(vec![
                        EventFilter::Property {
                            property_name: "Is Premium".to_string(),
                            property_type: PropertyType::User,
                            operation: PropValueOperation::Eq,
                            value: Some(vec![Value::Bool(true)]),
                        }]),
                    breakdowns: Some(vec![
                        Breakdown::Property {
                            property_name: "Device".to_string(),
                            property_type: PropertyType::User,
                        }]),
                    queries: vec![Query::CountEvents],
                },
                Event {
                    event_name: "Buy Product".to_string(),
                    event_type: EventType::Regular,
                    filters: None,
                    breakdowns: None,
                    queries: vec![
                        Query::CountEvents,
                        Query::CountUniqueGroups,
                        Query::CountPerGroup {
                            aggregate: AggregateFunction::Avg,
                        },
                        Query::AggregatePropertyPerGroup {
                            property_name: "Revenue".to_string(),
                            property_type: PropertyType::Event,
                            aggregate_per_group: PartitionedAggregateFunction::Sum,
                            aggregate: AggregateFunction::Avg,
                        },
                        Query::AggregateProperty {
                            property_name: "Revenue".to_string(),
                            property_type: PropertyType::Event,
                            aggregate: AggregateFunction::Sum,
                        },
                    ],
                }],
            filters: None,
            breakdowns: Some(vec![
                Breakdown::Property {
                    property_name: "Country".to_string(),
                    property_type: PropertyType::User,
                },
            ]),
            segments: None,
        };

        let cl = Client::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            "Content-Type",
            HeaderValue::from_str("application/json").unwrap(),
        );

        let body = serde_json::to_string(&es).unwrap();

        let resp = cl
            .post("http://127.0.0.1:8080/v1/projects/0/queries/event-segmentation")
            .body(body)
            .headers(headers.clone())
            .send()
            .await
            .unwrap();

        let status = resp.status();
        let txt = resp.text().await.unwrap();
        println!("{}", &txt);
        assert_eq!(status, StatusCode::OK);

        Ok(())
    }
}
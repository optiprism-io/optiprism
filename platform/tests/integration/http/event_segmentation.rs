#[cfg(test)]
mod tests {
    use axum::{Router, Server};
    use metadata::{Metadata, Store};
    use platform::error::Result;
    use platform::http::event_segmentation;
    use platform::EventSegmentationProvider;
    use query::QueryProvider;
    use std::env::temp_dir;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;
    use uuid::Uuid;

    use chrono::{DateTime, Utc};

    use arrow::datatypes::DataType as DFDataType;
    use datafusion::datasource::object_store::local::LocalFileSystem;
    use datafusion::prelude::CsvReadOptions;

    use axum::headers::{HeaderMap, HeaderValue};
    use axum::http::StatusCode;
    use datafusion::datasource::file_format::csv::CsvFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use metadata::database::{Column, Table, TableType};
    use metadata::properties::provider::Namespace;
    use metadata::properties::{CreatePropertyRequest, Property};
    use metadata::{events, properties};
    use platform::event_segmentation::types::{
        AggregateFunction, Analysis, Breakdown, ChartType, Event, EventFilter, EventSegmentation,
        EventType, PartitionedAggregateFunction, PropValueOperation, PropertyType, Query,
        QueryTime, TimeUnit,
    };
    use reqwest::Client;
    use serde_json::Value;

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
                    "event_created_at".to_string(),
                    DFDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
                    false,
                    None,
                ),
            )
            .await?;
        md.database
            .add_column(
                TableType::Events(org_id, proj_id),
                Column::new("event_event".to_string(), DFDataType::UInt16, false, None),
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
                is_system: false,
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
                is_system: false,
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
                typ: DFDataType::Float64,
                status: properties::Status::Enabled,
                is_system: false,
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
            create_entities(md.clone(), 1, 1).await.unwrap();

            let table = md
                .database
                .get_table(TableType::Events(1, 1))
                .await
                .unwrap();
            let schema = table.arrow_schema();
            let options = CsvReadOptions::new().schema(&schema);
            let path = "../tests/events.csv";
            let opt = ListingOptions::new(Arc::new(CsvFormat::default()));
            let config = ListingTableConfig::new(Arc::new(LocalFileSystem {}), path)
                .with_listing_options(opt)
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
            time: QueryTime::Between { from, to },
            group: "user_id".to_string(),
            interval_unit: TimeUnit::Minute,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![
                Event {
                    event_name: "View Product".to_string(),
                    event_type: EventType::Regular,
                    filters: Some(vec![EventFilter::Property {
                        property_name: "Is Premium".to_string(),
                        property_type: PropertyType::User,
                        operation: PropValueOperation::Eq,
                        value: Some(vec![Value::Bool(true)]),
                    }]),
                    breakdowns: Some(vec![Breakdown::Property {
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
                },
            ],
            filters: None,
            breakdowns: Some(vec![Breakdown::Property {
                property_name: "Country".to_string(),
                property_type: PropertyType::User,
            }]),
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
            .post("http://127.0.0.1:8080/v1/organizations/1/projects/1/queries/event-segmentation")
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

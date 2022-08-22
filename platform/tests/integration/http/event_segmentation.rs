#[cfg(test)]
mod tests {
    use axum::{Router, Server};
    use metadata::Metadata;
    use platform::error::Result;
    use platform::http::queries;
    use query::{Context, QueryProvider};
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
    use metadata::database::{Column, Table, TableRef};
    use metadata::properties::provider::Namespace;
    use metadata::properties::{CreatePropertyRequest, Property};
    use metadata::{events, properties};
    use platform::queries::event_segmentation::{
        Analysis, Breakdown, ChartType, Event, EventFilter, EventSegmentation, EventType,
        PropertyType, Query,
    };
    use platform::queries::types::{
        AggregateFunction, EventRef, PartitionedAggregateFunction, PropValueOperation, PropertyRef,
        QueryTime, TimeUnit,
    };
    use query::test_util::{create_entities, create_md, events_provider};
    use reqwest::Client;
    use serde_json::Value;

    #[tokio::test]
    async fn test_event_segmentation() -> Result<()> {
        tokio::spawn(async {
            let md = create_md().unwrap();

            let org_id = 1;
            let proj_id = 1;

            let ctx = Context {
                organization_id: org_id,
                account_id: 1,
                project_id: proj_id,
            };

            create_entities(md.clone(), org_id, proj_id).await.unwrap();
            let input = events_provider(md.database.clone(), org_id, proj_id)
                .await
                .unwrap();
            let query = QueryProvider::new_from_logical_plan(md, input);
            let app = queries::attach_routes(
                Router::new(),
                Arc::new(platform::queries::provider::QueryProvider::new(Arc::new(
                    query,
                ))),
            );

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
            group: "event_user_id".to_string(),
            interval_unit: TimeUnit::Minute,
            chart_type: ChartType::Line,
            analysis: Analysis::Linear,
            compare: None,
            events: vec![
                Event {
                    event: EventRef::Regular {
                        event_name: "View Product".to_string(),
                    },
                    filters: Some(vec![EventFilter::Property {
                        property: PropertyRef::User {
                            property_name: "Is Premium".to_string(),
                        },
                        operation: PropValueOperation::Eq,
                        value: Some(vec![Value::Bool(true)]),
                    }]),
                    breakdowns: Some(vec![Breakdown::Property {
                        property: PropertyRef::User {
                            property_name: "Device".to_string(),
                        },
                    }]),
                    queries: vec![Query::CountEvents],
                },
                Event {
                    event: EventRef::Regular {
                        event_name: "Buy Product".to_string(),
                    },
                    filters: None,
                    breakdowns: None,
                    queries: vec![
                        Query::CountEvents,
                        Query::CountUniqueGroups,
                        Query::CountPerGroup {
                            aggregate: AggregateFunction::Avg,
                        },
                        Query::AggregatePropertyPerGroup {
                            property: PropertyRef::Event {
                                property_name: "Revenue".to_string(),
                            },
                            aggregate_per_group: PartitionedAggregateFunction::Sum,
                            aggregate: AggregateFunction::Avg,
                        },
                        Query::AggregateProperty {
                            property: PropertyRef::Event {
                                property_name: "Revenue".to_string(),
                            },
                            aggregate: AggregateFunction::Sum,
                        },
                    ],
                },
            ],
            filters: None,
            breakdowns: Some(vec![Breakdown::Property {
                property: PropertyRef::User {
                    property_name: "Country".to_string(),
                },
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

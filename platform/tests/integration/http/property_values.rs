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
    use metadata::database::{Column, Table, TableType};
    use metadata::properties::provider::Namespace;
    use metadata::properties::{CreatePropertyRequest, Property};
    use metadata::{events, properties};
    use platform::queries::event_segmentation::{
        Analysis, Breakdown, ChartType, Event, EventFilter, EventSegmentation, EventType,
        PropertyType, Query,
    };
    use platform::queries::property_values::{Filter, PropertyValues};
    use platform::queries::types::{
        AggregateFunction, EventRef, PartitionedAggregateFunction, PropValueOperation, PropertyRef,
        QueryTime, TimeUnit,
    };
    use query::test_util::{create_entities, create_md, events_provider};
    use reqwest::Client;
    use serde_json::Value;

    #[tokio::test]
    async fn test_property_values() -> Result<()> {
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

        let req = PropertyValues {
            property: PropertyRef::Event {
                property_name: "Product Name".to_string(),
            },
            event: Some(EventRef::Regular {
                event_name: "View Product".to_string(),
            }),
            filter: Some(Filter {
                operation: PropValueOperation::Like,
                value: Some(vec![Value::String("goo%".to_string())]),
            }),
        };
        let cl = Client::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            "Content-Type",
            HeaderValue::from_str("application/json").unwrap(),
        );

        let body = serde_json::to_string(&req).unwrap();

        let resp = cl
            .post("http://127.0.0.1:8080/v1/organizations/1/projects/1/queries/property-values")
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

#[cfg(test)]
mod tests {
    use axum::{Router, Server};

    use platform::error::Result;
    use platform::http::queries;
    use query::{Context, QueryProvider};

    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    use axum::headers::{HeaderMap, HeaderValue};
    use axum::http::StatusCode;

    use platform::queries::property_values::{Filter, PropertyValues};
    use platform::queries::types::{EventRef, PropValueOperation, PropertyRef};
    use query::test_util::{create_entities, create_md, events_provider};
    use reqwest::Client;
    use serde_json::Value;
    use crate::http::tests::{create_admin_acc_and_login, run_http_service};

    #[tokio::test]
    async fn test_property_values() -> Result<()> {
        let (md, pp) = run_http_service(true).await?;
        let cl = Client::new();
        let headers = create_admin_acc_and_login(&pp.auth, &md.accounts, &cl).await?;

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
        assert_eq!(status, StatusCode::OK);

        Ok(())
    }
}

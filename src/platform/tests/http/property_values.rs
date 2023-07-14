#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use platform::queries::property_values::Filter;
    use platform::queries::property_values::ListPropertyValuesRequest;
    use platform::EventRef;
    use platform::PropValueOperation;
    use platform::PropertyRef;
    use reqwest::Client;
    use serde_json::Value;

    use crate::assert_response_status_eq;
    use crate::http::tests::create_admin_acc_and_login;
    use crate::http::tests::run_http_service;

    #[tokio::test]
    async fn test_property_values() -> anyhow::Result<()> {
        let (base_url, md, pp) = run_http_service(true).await?;
        let cl = Client::new();
        let headers = create_admin_acc_and_login(&pp.auth, &md.accounts).await?;
        let req = ListPropertyValuesRequest {
            property: PropertyRef::Event {
                property_name: "Product Name".to_string(),
            },
            event: Some(EventRef::Regular {
                event_name: "View Product".to_string(),
            }),
            filter: Some(Filter {
                operation: PropValueOperation::Eq,
                value: Some(vec![Value::String("goo%".to_string())]),
            }),
        };

        let resp = cl
            .post(format!(
                "{base_url}/organizations/1/projects/1/property-values"
            ))
            .body(serde_json::to_string(&req)?)
            .headers(headers.clone())
            .send()
            .await?;

        assert_response_status_eq!(resp, StatusCode::OK);
        let _txt = resp.text().await?;

        Ok(())
    }
}

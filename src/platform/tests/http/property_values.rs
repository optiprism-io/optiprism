#[cfg(test)]
mod tests {

    use axum::http::StatusCode;
    use platform::queries::property_values::Filter;
    use platform::queries::property_values::PropertyValues;
    use platform::queries::types::EventRef;
    use platform::queries::types::PropValueOperation;
    use platform::queries::types::PropertyRef;
    use reqwest::Client;
    use serde_json::Value;

    use crate::assert_response_status;
    use crate::http::tests::create_admin_acc_and_login;
    use crate::http::tests::run_http_service;

    #[tokio::test]
    async fn test_property_values() -> anyhow::Result<()> {
        let (base_url, md, pp) = run_http_service(true).await?;
        let cl = Client::new();
        let headers = create_admin_acc_and_login(&pp.auth, &md.accounts).await?;

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

        let resp = cl
            .post(format!(
                "{base_url}/organizations/1/projects/1/queries/property-values"
            ))
            .body(serde_json::to_string(&req)?)
            .headers(headers.clone())
            .send()
            .await?;

        assert_response_status!(resp, StatusCode::OK);
        let _txt = resp.text().await?;

        Ok(())
    }
}

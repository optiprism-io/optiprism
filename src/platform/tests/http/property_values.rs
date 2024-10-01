#[cfg(test)]
mod tests {
    use platform::properties::Filter;
    use platform::properties::ListPropertyValuesRequest;
    use platform::EventRef;
    use platform::PropValueOperation;
    use platform::PropertyRef;
    use reqwest::Client;
    use reqwest::StatusCode;
    use serde_json::Value;

    use crate::assert_response_status_eq;
    use crate::http::tests::init_settings;
    use crate::http::tests::login;
    use crate::http::tests::run_http_service;

    #[tokio::test]
    async fn test_property_values() {
        let (base_url, md, pp) = run_http_service(true).await.unwrap();
        let cl = Client::new();
        init_settings(&md);
        let headers = login(&pp.auth, &md.accounts).await.unwrap();
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
            .post(format!("{base_url}/projects/1/property-values"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
        let _txt = resp.text().await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use chrono::Utc;
    use platform::event_records::EventRecordsSearchRequest;
    use platform::QueryTime;
    use reqwest::Client;
    use reqwest::StatusCode;

    use crate::assert_response_status_eq;
    use crate::http::tests::create_admin_acc_and_login;
    use crate::http::tests::run_http_service;

    #[tokio::test]
    async fn test_event_records_search() {
        let (base_url, md, pp) = run_http_service(true).await.unwrap();
        let cl = Client::new();
        let headers = create_admin_acc_and_login(&pp.auth, &md.accounts)
            .await
            .unwrap();
        let from = DateTime::parse_from_rfc3339("2021-09-08T13:42:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let to = DateTime::parse_from_rfc3339("2021-09-08T13:48:00.000000+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let req = EventRecordsSearchRequest {
            time: QueryTime::Between { from, to },
            events: None,
            filters: None,
            properties: None,
        };

        let resp = cl
            .post(format!("{base_url}/projects/1/event-records/search"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
        let _txt = resp.text().await.unwrap();
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Client;
    use reqwest::StatusCode;

    use crate::assert_response_status_eq;
    use crate::http::tests::init_settings;
    use crate::http::tests::login;
    use crate::http::tests::run_http_service;

    #[tokio::test]
    async fn test_projects() {
        let (base_url, md, pp) = run_http_service(true).await.unwrap();
        let cl = Client::new();
        init_settings(&md);
        let headers = login(&pp.auth, &md.accounts).await.unwrap();

        let resp = cl
            .get(format!("{base_url}/projects"))
            .headers(headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
    }
}

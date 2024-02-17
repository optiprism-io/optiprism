#[cfg(test)]
mod tests {
    use axum::http::StatusCode;
    use reqwest::Client;

    use crate::assert_response_status_eq;
    use crate::http::tests::create_admin_acc_and_login;
    use crate::http::tests::run_http_service;

    #[tokio::test]
    async fn test_projects() {
        let (base_url, md, pp) = run_http_service(true).await.unwrap();
        let cl = Client::new();
        let headers = create_admin_acc_and_login(&pp.auth, &md.accounts)
            .await
            .unwrap();

        let resp = cl
            .get(format!("{base_url}/projects"))
            .headers(headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
    }
}
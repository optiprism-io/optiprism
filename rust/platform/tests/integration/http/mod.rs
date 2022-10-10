mod auth;
mod custom_events;
mod event_segmentation;
mod events;
mod properties;
mod property_values;

#[cfg(test)]
mod tests {
    use std::env::temp_dir;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use axum::headers::{HeaderMap, HeaderValue};
    use axum::http;
    use axum::http::StatusCode;
    use chrono::Duration;
    use datafusion::datasource::{MemTable, TableProvider};
    use lazy_static::lazy_static;
    use reqwest::Client;
    use uuid::Uuid;
    use common::rbac::OrganizationRole;
    use metadata::MetadataProvider;
    use metadata::store::Store;
    use platform::auth::password::make_password_hash;
    use platform::auth::types::TokensResponse;
    use platform::http::auth::{COOKIE_NAME_REFRESH_TOKEN, LogInRequest};
    use platform::PlatformProvider;
    use query::QueryProvider;
    use query::test_util::{create_entities, empty_provider, events_provider};
    use std::sync::atomic::{Ordering, AtomicU16};

    lazy_static! {
        static ref HTTP_PORT:AtomicU16 = AtomicU16::new(8080);
    }

    pub fn tmp_store() -> Arc<Store> {
        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));
        Arc::new(Store::new(path))
    }

    pub async fn create_admin_acc_and_login(
        auth: &Arc<platform::auth::Provider>,
        md_acc: &Arc<metadata::accounts::Provider>,
        cl: &Client,
    ) -> anyhow::Result<HeaderMap> {
        let pwd = "password";

        let admin = md_acc
            .create(metadata::accounts::CreateAccountRequest {
                created_by: Some(1),
                password_hash: make_password_hash(pwd)?.to_string(),
                email: "admin@mail.com".to_string(),
                first_name: None,
                last_name: None,
                role: None,
                organizations: Some(vec![(1, OrganizationRole::Admin)]),
                projects: None,
                teams: None,
            })
            .await?;

        let tokens = auth.log_in(admin.email.as_str(), pwd).await?;
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_str("application/json")?,
        );
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {}", tokens.access_token).as_str())?,
        );

        Ok(headers)
    }

    pub async fn run_http_service(create_test_data: bool) -> anyhow::Result<(String, Arc<metadata::MetadataProvider>, Arc<platform::PlatformProvider>)> {
        let md = Arc::new(MetadataProvider::try_new(tmp_store())?);
        let input = if create_test_data {
            create_entities(md.clone(), 1, 1).await?;
            events_provider(md.database.clone(), 1, 1)
                .await?
        } else {
            empty_provider()?
        };

        let query = Arc::new(QueryProvider::new_from_logical_plan(md.clone(), input));
        let p_query = Arc::new(platform::queries::provider::QueryProvider::new(query));

        let pp = Arc::new(PlatformProvider::new(
            md.clone(),
            p_query.clone(),
            Duration::days(1),
            "access_secret".to_string(),
            Duration::days(1),
            "refresh_secret".to_string(),
        ));

        let addr = SocketAddr::from(([127, 0, 0, 1], HTTP_PORT.fetch_add(1, Ordering::SeqCst)));
        let svc = platform::http::Service::new(&md, &pp, addr.clone());
        svc.run().await;

        let base_addr = format!("http://{:?}:{:?}", addr.ip(), addr.port());

        Ok((base_addr, md, pp))
    }
}
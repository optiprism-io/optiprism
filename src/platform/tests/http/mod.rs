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
    use std::sync::atomic::AtomicU16;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use axum::headers::HeaderMap;
    use axum::headers::HeaderValue;
    use axum::http;
    use chrono::Duration;
    use common::rbac::OrganizationRole;
    use lazy_static::lazy_static;
    use metadata::store::Store;
    use metadata::MetadataProvider;
    use platform::auth;
    use platform::auth::password::make_password_hash;
    use platform::auth::LogInRequest;
    use platform::PlatformProvider;
    use query::test_util::create_entities;
    use query::test_util::empty_provider;
    use query::test_util::events_provider;
    use query::ProviderImpl;
    use serde_json::json;
    use tokio::time::sleep;
    use uuid::Uuid;

    lazy_static! {
        pub static ref EMPTY_LIST: serde_json::Value = json!({"data":[],"meta":{"next":null}});
        pub static ref AUTH_CFG: auth::Config = auth::Config {
            access_token_duration: Duration::days(1),
            access_token_key: "access_secret".to_string(),
            refresh_token_duration: Duration::days(1),
            refresh_token_key: "refresh_secret".to_string(),
        };
    }
    static HTTP_PORT: AtomicU16 = AtomicU16::new(8080);

    pub fn tmp_store() -> Arc<Store> {
        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));
        Arc::new(Store::new(path))
    }

    pub async fn create_admin_acc_and_login(
        auth: &Arc<dyn platform::auth::Provider>,
        md_acc: &Arc<dyn metadata::accounts::Provider>,
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

        let tokens = auth
            .log_in(LogInRequest {
                email: admin.email,
                password: pwd.to_string(),
            })
            .await?;
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

    pub async fn run_http_service(
        create_test_data: bool,
    ) -> anyhow::Result<(
        String,
        Arc<metadata::MetadataProvider>,
        Arc<platform::PlatformProvider>,
    )> {
        let md = Arc::new(MetadataProvider::try_new(tmp_store())?);
        let input = if create_test_data {
            create_entities(md.clone(), 1, 1).await?;
            events_provider(md.database.clone(), 1, 1).await?
        } else {
            empty_provider()?
        };

        let query = Arc::new(ProviderImpl::new_from_logical_plan(md.clone(), input));

        let platform_provider =
            Arc::new(PlatformProvider::new(md.clone(), query, AUTH_CFG.clone()));

        let addr = SocketAddr::from(([127, 0, 0, 1], HTTP_PORT.fetch_add(1, Ordering::SeqCst)));
        let svc =
            platform::http::Service::new(&md, &platform_provider, AUTH_CFG.clone(), addr);

        tokio::spawn(async move {
            svc.serve().await.unwrap();
        });

        sleep(tokio::time::Duration::from_millis(100)).await;

        let base_addr = format!("http://{:?}:{:?}/api/v1", addr.ip(), addr.port());

        Ok((base_addr, md, platform_provider))
    }

    #[macro_export]
    macro_rules! assert_response_status_eq {
        ($resp:expr,$status:expr) => {{ assert_eq!($resp.status(), $status, "{}", $resp.text().await?.as_str()) }};
    }

    #[macro_export]
    macro_rules! assert_response_json_eq {
        ($resp:expr, $body:expr) => {{ assert_eq!($resp.text().await?, $body.to_string()) }};
    }
}

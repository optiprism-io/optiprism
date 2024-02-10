mod auth;
mod custom_events;
mod dashboards;
mod event_records_search;
mod event_segmentation;
mod events;
mod projects;
mod properties;
mod property_values;
mod reports;

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
    use axum::Router;
    use chrono::Duration;
    use common::rbac::OrganizationRole;
    
    use hyper::Server;
    use lazy_static::lazy_static;
    use metadata::accounts::Accounts;
    use metadata::MetadataProvider;
    use platform::auth::password::make_password_hash;
    use platform::auth::provider::Config;
    use platform::auth::provider::LogInRequest;
    use platform::auth::Auth;
    use platform::http::attach_routes;
    use platform::PlatformProvider;
    use query::test_util::create_entities;
    use query::QueryProvider;
    use serde_json::json;
    use storage::db::OptiDBImpl;
    use storage::db::Options;
    use tokio::time::sleep;
    use uuid::Uuid;

    lazy_static! {
        pub static ref EMPTY_LIST: serde_json::Value = json!({"data":[],"meta":{"next":null}});
        pub static ref AUTH_CFG: Config = Config {
            access_token_duration: Duration::days(1),
            access_token_key: "access_secret".to_string(),
            refresh_token_duration: Duration::days(1),
            refresh_token_key: "refresh_secret".to_string(),
        };
    }
    static HTTP_PORT: AtomicU16 = AtomicU16::new(8080);

    pub async fn create_admin_acc_and_login(
        auth: &Arc<Auth>,
        md_acc: &Arc<Accounts>,
    ) -> anyhow::Result<HeaderMap> {
        let pwd = "password";

        let admin = md_acc.create(metadata::accounts::CreateAccountRequest {
            created_by: Some(1),
            password_hash: make_password_hash(pwd)?.to_string(),
            email: "admin@mail.com".to_string(),
            name: Some("name".to_string()),
            role: None,
            organizations: Some(vec![(1, OrganizationRole::Admin)]),
            projects: None,
            teams: None,
        })?;

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
        let mut path = temp_dir();
        path.push(format!("{}", Uuid::new_v4()));
        let rocks = Arc::new(metadata::rocksdb::new(path.join("md"))?);
        let db = Arc::new(OptiDBImpl::open(path.join("store"), Options {})?);
        let md = Arc::new(MetadataProvider::try_new(rocks, db.clone())?);
        db.create_table("events".to_string(), storage::table::Options::test())?;

        if create_test_data {
            create_entities(md.clone(), &db, 1).await?;
        }
        let query_provider = Arc::new(QueryProvider::new(md.clone(), db.clone()));

        let platform_provider = Arc::new(PlatformProvider::new(
            md.clone(),
            query_provider,
            AUTH_CFG.clone(),
        ));

        let addr = SocketAddr::from(([127, 0, 0, 1], HTTP_PORT.fetch_add(1, Ordering::SeqCst)));
        let router = attach_routes(
            Router::new(),
            &md,
            &platform_provider,
            AUTH_CFG.clone(),
            None,
        );
        tokio::spawn(async move {
            Server::bind(&addr)
                .serve(router.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .unwrap();
        });

        sleep(tokio::time::Duration::from_millis(100)).await;

        let base_addr = format!("http://{:?}:{:?}/api/v1", addr.ip(), addr.port());

        Ok((base_addr, md, platform_provider))
    }

    #[macro_export]
    macro_rules! assert_response_status_eq {
        ($resp:expr,$status:expr) => {{
            assert_eq!(
                $resp.status(),
                $status,
                "{}",
                $resp.text().await.unwrap().as_str()
            )
        }};
    }

    #[macro_export]
    macro_rules! assert_response_json_eq {
        ($resp:expr, $body:expr) => {{ assert_eq!($resp.text().await.unwrap(), $body.to_string()) }};
    }
}

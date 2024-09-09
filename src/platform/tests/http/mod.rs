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
    use std::str::FromStr;
    use std::sync::atomic::AtomicU16;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use axum::Router;
    use chrono::Duration;
    use common::config::Config;
    use common::rbac::OrganizationRole;
    use lazy_static::lazy_static;
    use metadata::accounts::Accounts;
    use metadata::MetadataProvider;
    use platform::auth::password::make_password_hash;
    use platform::auth::provider::LogInRequest;
    use platform::auth::Auth;
    use platform::http::attach_routes;
    use query::test_util::create_entities;
    use reqwest::header::HeaderMap;
    use reqwest::header::HeaderValue;
    use reqwest::header::AUTHORIZATION;
    use reqwest::header::CONTENT_TYPE;
    use serde_json::json;
    use storage::db::OptiDBImpl;
    use storage::db::Options;
    use tokio::time::sleep;
    use tracing::level_filters::LevelFilter;
    use uuid::Uuid;
    use common::config;
    use query::event_records::EventRecordsProvider;
    use query::event_segmentation::EventSegmentationProvider;
    use query::funnel::FunnelProvider;
    use query::group_records::GroupRecordsProvider;
    use query::properties::PropertiesProvider;

    lazy_static! {
        pub static ref EMPTY_LIST: serde_json::Value = json!({"data":[],"meta":{"next":null}});
        pub static ref AUTH_CFG: Config = Config {
            server: config::Server { host: SocketAddr::from_str(":8080").unwrap()},
            data: config::Data {
 path: Default::default(),ua_db_path: Default::default(),geo_city_path: Default::default(),ui_path: Default::default(),},
            auth: config::Auth { access_token_duration: Duration::days(1),refresh_token_duration: Duration::days(1)},
            misc: config::Misc { session_cleaner_interval: Default::default(),project_default_session_duration: Default::default()},
            events_table: config::Table {
 levels: 0,l0_max_parts: 0,l1_max_size_bytes: 0,level_size_multiplier: 0,max_log_length_bytes: 0,merge_max_l1_part_size_bytes: 0,merge_part_size_multiplier: 0,merge_data_page_size_limit_bytes: 0,merge_row_group_values_limit: 0,merge_array_size: 0,merge_chunk_size: 0,merge_array_page_size: 0,merge_max_page_size: 0,},
            group_table: config::Table {
 levels: 0,l0_max_parts: 0,l1_max_size_bytes: 0,level_size_multiplier: 0,max_log_length_bytes: 0,merge_max_l1_part_size_bytes: 0,merge_part_size_multiplier: 0,merge_data_page_size_limit_bytes: 0,merge_row_group_values_limit: 0,merge_array_size: 0,merge_chunk_size: 0,merge_array_page_size: 0,merge_max_page_size: 0,},
        log: config::Log { level: LevelFilter::INFO},};
    }
    static HTTP_PORT: AtomicU16 = AtomicU16::new(8080);

    pub async fn create_admin_acc_and_login(
        auth: &Arc<Auth>,
        md_acc: &Arc<Accounts>,
    ) -> anyhow::Result<HeaderMap> {
        let pwd = "password";

        let admin = md_acc.create(metadata::accounts::CreateAccountRequest {
            created_by: 1,
            password_hash: make_password_hash(pwd)?.to_string(),
            email: "admin@mail.com".to_string(),
            name: Some("name".to_string()),
            force_update_password: false,
            force_update_email: false,
            role: None,
            organizations: Some(vec![(1, OrganizationRole::Admin)]),
            projects: None,
            teams: None,
        })?;

        let tokens = auth
            .log_in(LogInRequest {
                email: admin.email,
                password: pwd.to_string(),
            },None)
            .await?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_str("application/json")?);
        headers.insert(
            AUTHORIZATION,
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
        db.create_table("events".to_string(), storage::table::Options::test(false))?;

        if create_test_data {
            create_entities(md.clone(), &db, 1).await?;
        }
        let es_prov = Arc::new(EventSegmentationProvider::new(md.clone(), db.clone()));
        let funnel_prov = Arc::new(FunnelProvider::new(md.clone(), db.clone()));
        let prop_prov = Arc::new(PropertiesProvider::new(md.clone(), db.clone()));
        let er_prov = Arc::new(EventRecordsProvider::new(md.clone(), db.clone()));
        let gr_prov = Arc::new(GroupRecordsProvider::new(md.clone(), db.clone()));

        let platform_provider = Arc::new(platform::PlatformProvider::new(
            md.clone(),
            es_prov,
            funnel_prov,
            prop_prov,
            er_prov,
            gr_prov,
            AUTH_CFG.clone(),
        ));

        let addr = SocketAddr::from(([127, 0, 0, 1], HTTP_PORT.fetch_add(1, Ordering::SeqCst)));
        let router = attach_routes(Router::new(), &md, &platform_provider, AUTH_CFG.clone());
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
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

// mod auth;
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
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::atomic::AtomicU16;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use axum::Router;
    use chrono::Duration;
    use common::config;
    use common::config::Config;
    use common::rbac::Role;
    use common::ADMIN_ID;
    use lazy_static::lazy_static;
    use metadata::accounts::Accounts;
    use metadata::accounts::CreateAccountRequest;
    use metadata::error::MetadataError;
    use metadata::organizations::CreateOrganizationRequest;
    use metadata::projects::CreateProjectRequest;
    use metadata::util::init_db;
    use metadata::MetadataProvider;
    use platform::auth::password::make_password_hash;
    use platform::auth::provider::LogInRequest;
    use platform::auth::Auth;
    use platform::http::attach_routes;
    use query::event_records::EventRecordsProvider;
    use query::event_segmentation::EventSegmentationProvider;
    use query::funnel::FunnelProvider;
    use query::group_records::GroupRecordsProvider;
    use query::properties::PropertiesProvider;
    use query::test_util::create_entities;
    use rand::distributions::Alphanumeric;
    use rand::distributions::DistString;
    use rand::thread_rng;
    use reqwest::header::HeaderMap;
    use reqwest::header::HeaderValue;
    use reqwest::header::AUTHORIZATION;
    use reqwest::header::CONTENT_TYPE;
    use serde_json::json;
    use tokio::time::sleep;
    use tracing::level_filters::LevelFilter;

    lazy_static! {
        pub static ref EMPTY_LIST: serde_json::Value = json!({"data":[],"meta":{"next":null}});
        pub static ref AUTH_CFG: Config = Config {
            server: config::Server {
                host: SocketAddr::from_str("0.0.0.0:8080").unwrap()
            },
            data: config::Data {
                path: Default::default(),
                ua_db_path: Default::default(),
                geo_city_path: Default::default(),
                ui_path: Default::default(),
            },
            auth: config::Auth {
                access_token_duration: Duration::days(1),
                refresh_token_duration: Duration::days(1)
            },
            misc: config::Misc {
                session_cleaner_interval: Default::default(),
                project_default_session_duration: Default::default()
            },
            events_table: config::Table {
                levels: 0,
                l0_max_parts: 0,
                l1_max_size_bytes: 0,
                level_size_multiplier: 0,
                max_log_length_bytes: 0,
                merge_max_l1_part_size_bytes: 0,
                merge_part_size_multiplier: 0,
                merge_data_page_size_limit_bytes: 0,
                merge_row_group_values_limit: 0,
                merge_array_size: 0,
                merge_chunk_size: 0,
                merge_array_page_size: 0,
                merge_max_page_size: 0,
            },
            group_table: config::Table {
                levels: 0,
                l0_max_parts: 0,
                l1_max_size_bytes: 0,
                level_size_multiplier: 0,
                max_log_length_bytes: 0,
                merge_max_l1_part_size_bytes: 0,
                merge_part_size_multiplier: 0,
                merge_data_page_size_limit_bytes: 0,
                merge_row_group_values_limit: 0,
                merge_array_size: 0,
                merge_chunk_size: 0,
                merge_array_page_size: 0,
                merge_max_page_size: 0,
            },
            log: config::Log {
                level: LevelFilter::INFO
            },
        };
    }
    static HTTP_PORT: AtomicU16 = AtomicU16::new(8081);

    pub async fn login(auth: &Arc<Auth>, _md_acc: &Arc<Accounts>) -> anyhow::Result<HeaderMap> {
        let tokens = auth
            .log_in(
                LogInRequest {
                    email: "admin@admin.com".to_owned(),
                    password: "admin".to_owned(),
                },
                None,
            )
            .await?;
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_str("application/json")?);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(format!("Bearer {}", tokens.access_token).as_str())?,
        );

        Ok(headers)
    }

    pub fn init_settings(md: &Arc<MetadataProvider>) {
        let mut settings = match md.settings.load() {
            Ok(cfg) => cfg,
            Err(MetadataError::NotFound(_)) => {
                let cfg = metadata::settings::Settings::default();
                md.settings.save(&cfg).unwrap();
                cfg
            }
            Err(err) => panic!("{}", err.to_string()),
        };

        if settings.auth_access_token.is_empty() {
            settings.auth_access_token = "test".to_string();
        }
        if settings.auth_refresh_token.is_empty() {
            settings.auth_refresh_token = "test".to_string();
        }

        md.settings.save(&settings).unwrap();
    }

    pub async fn run_http_service(
        create_test_data: bool,
    ) -> anyhow::Result<(
        String,
        Arc<metadata::MetadataProvider>,
        Arc<platform::PlatformProvider>,
    )> {
        let (md, db) = init_db().unwrap();

        let admin = match md.accounts.create(CreateAccountRequest {
            created_by: ADMIN_ID,
            password_hash: make_password_hash("admin")?,
            email: "admin@admin.com".to_string(),
            name: Some("admin".to_string()),
            force_update_password: true,
            force_update_email: true,
            role: Some(Role::Admin),
            organizations: None,
            projects: None,
            teams: None,
        }) {
            Ok(acc) => acc,
            Err(_err) => md.accounts.get_by_email("admin@admin.com")?,
        };
        let org = match md.organizations.create(CreateOrganizationRequest {
            created_by: admin.id,
            name: "My Organization".to_string(),
        }) {
            Ok(org) => org,
            Err(_err) => md.organizations.get_by_id(1)?,
        };

        let token = Alphanumeric.sample_string(&mut thread_rng(), 64);

        let _proj = match md.projects.create(CreateProjectRequest {
            created_by: admin.id,
            organization_id: org.id,
            name: "My Project".to_string(),
            description: None,
            tags: None,
            token: token.clone(),
            session_duration_seconds: 60 * 60 * 24,
        }) {
            Ok(proj) => proj,
            Err(_err) => md.projects.get_by_id(1)?,
        };/*
        md.dictionaries.create_key(
            proj.id,
            TABLE_EVENTS,
            "project_id",
            proj.id,
            proj.name.as_str(),
        )?;
        for g in 0..GROUPS_COUNT {
            md.dictionaries.create_key(
                proj.id,
                group_col(g).as_str(),
                "project_id",
                proj.id,
                proj.name.as_str(),
            )?;
        }
*/
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
        dbg!(addr);
        let router = attach_routes(Router::new(), &md, &platform_provider, AUTH_CFG.clone());
        let listener = tokio::net::TcpListener::bind(addr).await?;
        tokio::spawn(async move {
            axum::serve(
                listener,
                router.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
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

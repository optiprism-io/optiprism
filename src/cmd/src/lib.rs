use std::sync::Arc;

use ::store::db::OptiDBImpl;
use ::store::error::StoreError;
use ::store::table::Options as TableOptions;
use common::defaults::SESSION_DURATION;
use common::rbac::OrganizationRole;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::types::DType;
use common::types::COLUMN_CREATED_AT;
use common::types::COLUMN_EVENT;
use common::types::COLUMN_EVENT_ID;
use common::types::COLUMN_PROJECT_ID;
use common::types::COLUMN_USER_ID;
use common::types::EVENT_CLICK;
use common::types::EVENT_PAGE;
use common::types::EVENT_PROPERTY_A_CLASS;
use common::types::EVENT_PROPERTY_A_HREF;
use common::types::EVENT_PROPERTY_A_ID;
use common::types::EVENT_PROPERTY_A_NAME;
use common::types::EVENT_PROPERTY_A_STYLE;
use common::types::EVENT_PROPERTY_PAGE_PATH;
use common::types::EVENT_PROPERTY_PAGE_REFERER;
use common::types::EVENT_PROPERTY_PAGE_SEARCH;
use common::types::EVENT_PROPERTY_PAGE_TITLE;
use common::types::EVENT_PROPERTY_PAGE_URL;
use common::types::EVENT_SCREEN;
use common::types::USER_PROPERTY_CITY;
use common::types::USER_PROPERTY_CLIENT_FAMILY;
use common::types::USER_PROPERTY_CLIENT_VERSION_MAJOR;
use common::types::USER_PROPERTY_CLIENT_VERSION_MINOR;
use common::types::USER_PROPERTY_CLIENT_VERSION_PATCH;
use common::types::USER_PROPERTY_COUNTRY;
use common::types::USER_PROPERTY_DEVICE_BRAND;
use common::types::USER_PROPERTY_DEVICE_FAMILY;
use common::types::USER_PROPERTY_DEVICE_MODEL;
use common::types::USER_PROPERTY_OS;
use common::types::USER_PROPERTY_OS_FAMILY;
use common::types::USER_PROPERTY_OS_VERSION_MAJOR;
use common::types::USER_PROPERTY_OS_VERSION_MINOR;
use common::types::USER_PROPERTY_OS_VERSION_PATCH;
use common::types::USER_PROPERTY_OS_VERSION_PATCH_MINOR;
use metadata::accounts::CreateAccountRequest;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::properties::DictionaryType;
use metadata::properties::Type;
use metadata::test_util::create_event;
use metadata::test_util::create_property;
use metadata::test_util::CreatePropertyMainRequest;
use metadata::MetadataProvider;
use metrics::describe_counter;
use metrics::describe_histogram;
use metrics::Unit;
use metrics_exporter_prometheus::PrometheusBuilder;
use platform::auth::password::make_password_hash;
use tracing::info;

pub mod error;
pub mod store;
pub mod test;

pub fn init_metrics() {
    let builder = PrometheusBuilder::new();
    builder
        .install()
        .expect("failed to install Prometheus recorder");

    describe_counter!("store.inserts_total", "number of inserts processed");
    describe_histogram!("store.insert_time_seconds", Unit::Seconds, "insert time");
    describe_counter!("store.scans_total", "number of scans processed");
    describe_counter!("store.scan_merges_total", "number of merges during scan");
    describe_histogram!("store.scan_time_seconds", Unit::Microseconds, "scan time");
    describe_histogram!(
        "store.scan_memtable_seconds",
        Unit::Seconds,
        "scan memtable time"
    );
    describe_counter!("store.compactions_total", "number of compactions");
    describe_histogram!(
        "store.compaction_time_seconds",
        Unit::Seconds,
        "compaction time"
    );
    describe_histogram!(
        "store.recovery_time_seconds",
        Unit::Seconds,
        "recovery time"
    );

    describe_histogram!("store.flush_time_seconds", Unit::Seconds, "recovery time");
}

pub fn init_system(
    md: &Arc<MetadataProvider>,
    db: &Arc<OptiDBImpl>,
    partitions: usize,
) -> error::Result<()> {
    let topts = TableOptions {
        levels: 7,
        merge_array_size: 10000,
        partitions,
        index_cols: 2,
        l1_max_size_bytes: 1024 * 1024 * 10,
        level_size_multiplier: 10,
        l0_max_parts: 4,
        max_log_length_bytes: 1024 * 1024 * 100,
        merge_array_page_size: 100000,
        merge_data_page_size_limit_bytes: Some(1024 * 1024 * 1000),
        merge_index_cols: 2,
        merge_max_l1_part_size_bytes: 1024 * 1024 * 10,
        merge_part_size_multiplier: 10,
        merge_row_group_values_limit: 1000,
        merge_chunk_size: 1024 * 8 * 8,
        merge_max_page_size: 1024 * 1024 * 10,
    };
    match db.create_table("events".to_string(), topts) {
        Ok(_) => {}
        Err(err) => match err {
            StoreError::AlreadyExists(_) => {}
            other => return Err(other.into()),
        },
    }

    create_property(md, 0, 0, CreatePropertyMainRequest {
        name: COLUMN_PROJECT_ID.to_string(),
        typ: Type::System,
        data_type: DType::Int64,
        nullable: false,
        dict: None,
    })?;

    create_property(md, 0, 0, CreatePropertyMainRequest {
        name: COLUMN_USER_ID.to_string(),
        typ: Type::System,
        data_type: DType::Int64,
        nullable: false,
        dict: None,
    })?;

    create_property(md, 0, 0, CreatePropertyMainRequest {
        name: COLUMN_CREATED_AT.to_string(),
        typ: Type::System,
        data_type: DType::Timestamp,
        nullable: false,
        dict: None,
    })?;

    create_property(md, 0, 0, CreatePropertyMainRequest {
        name: COLUMN_EVENT_ID.to_string(),
        typ: Type::System,
        data_type: DType::Int64,
        nullable: false,
        dict: None,
    })?;

    create_property(md, 0, 0, CreatePropertyMainRequest {
        name: COLUMN_EVENT.to_string(),
        typ: Type::System,
        data_type: DType::String,
        nullable: false,
        dict: Some(DictionaryType::Int64),
    })?;

    Ok(())
}

pub fn init_project(org_id: u64, project_id: u64, md: &Arc<MetadataProvider>) -> error::Result<()> {
    create_event(md, org_id, project_id, EVENT_CLICK.to_string())?;
    create_event(md, org_id, project_id, EVENT_PAGE.to_string())?;
    create_event(md, org_id, project_id, EVENT_SCREEN.to_string())?;
    let user_props = vec![
        USER_PROPERTY_CLIENT_FAMILY,
        USER_PROPERTY_CLIENT_VERSION_MINOR,
        USER_PROPERTY_CLIENT_VERSION_MAJOR,
        USER_PROPERTY_CLIENT_VERSION_PATCH,
        USER_PROPERTY_DEVICE_FAMILY,
        USER_PROPERTY_DEVICE_BRAND,
        USER_PROPERTY_DEVICE_MODEL,
        USER_PROPERTY_OS,
        USER_PROPERTY_OS_FAMILY,
        USER_PROPERTY_OS_VERSION_MAJOR,
        USER_PROPERTY_OS_VERSION_MINOR,
        USER_PROPERTY_OS_VERSION_PATCH,
        USER_PROPERTY_OS_VERSION_PATCH_MINOR,
        USER_PROPERTY_COUNTRY,
        USER_PROPERTY_CITY,
    ];
    for prop in user_props {
        create_property(md, org_id, project_id, CreatePropertyMainRequest {
            name: prop.to_string(),
            typ: Type::User,
            data_type: DType::String,
            nullable: true,
            dict: Some(DictionaryType::Int64),
        })?;
    }

    let event_props = vec![
        EVENT_PROPERTY_A_NAME,
        EVENT_PROPERTY_A_HREF,
        EVENT_PROPERTY_A_ID,
        EVENT_PROPERTY_A_CLASS,
        EVENT_PROPERTY_A_STYLE,
        EVENT_PROPERTY_PAGE_PATH,
        EVENT_PROPERTY_PAGE_REFERER,
        EVENT_PROPERTY_PAGE_SEARCH,
        EVENT_PROPERTY_PAGE_TITLE,
        EVENT_PROPERTY_PAGE_URL,
    ];

    for prop in event_props {
        create_property(md, org_id, project_id, CreatePropertyMainRequest {
            name: prop.to_string(),
            typ: Type::Event,
            data_type: DType::String,
            nullable: true,
            dict: None,
        })?;
    }

    Ok(())
}

fn init_test_org_structure(md: &Arc<MetadataProvider>) -> crate::error::Result<(u64, u64)> {
    let admin = match md.accounts.create(CreateAccountRequest {
        created_by: None,
        password_hash: make_password_hash("admin")?,
        email: "admin@email.com".to_string(),
        name: Some("admin".to_string()),
        role: Some(Role::Admin),
        organizations: None,
        projects: None,
        teams: None,
    }) {
        Ok(acc) => acc,
        Err(_err) => md.accounts.get_by_email("admin@email.com")?,
    };
    let org = match md.organizations.create(CreateOrganizationRequest {
        created_by: admin.id,
        name: "Test Organization".to_string(),
    }) {
        Ok(org) => org,
        Err(_err) => md.organizations.get_by_id(1)?,
    };

    let proj = match md.projects.create(org.id, CreateProjectRequest {
        created_by: admin.id,
        name: "Test Project".to_string(),
        description: None,
        tags: None,
        session_duration: SESSION_DURATION.num_seconds() as u64,
    }) {
        Ok(proj) => proj,
        Err(_err) => md.projects.get_by_id(1, 1)?,
    };

    info!("token: {}", proj.token);
    let _user = match md.accounts.create(CreateAccountRequest {
        created_by: Some(admin.id),
        password_hash: make_password_hash("test")?,
        email: "user@test.com".to_string(),
        name: Some("user".to_string()),
        role: None,
        organizations: Some(vec![(org.id, OrganizationRole::Member)]),
        projects: Some(vec![(proj.id, ProjectRole::Reader)]),
        teams: None,
    }) {
        Ok(acc) => acc,
        Err(_err) => md.accounts.get_by_email("user@test.com")?,
    };

    Ok((org.id, proj.id))
}

use std::sync::Arc;
use tracing::info;
use ::store::db::{OptiDBImpl, TableOptions};
use common::rbac::{OrganizationRole, ProjectRole, Role};
use common::types::{COLUMN_CREATED_AT, COLUMN_EVENT, COLUMN_EVENT_ID, COLUMN_PROJECT_ID, COLUMN_USER_ID, DType, EVENT_CLICK, EVENT_PAGE, EVENT_PROPERTY_A_CLASS, EVENT_PROPERTY_A_HREF, EVENT_PROPERTY_A_ID, EVENT_PROPERTY_A_NAME, EVENT_PROPERTY_A_STYLE, EVENT_PROPERTY_PAGE_PATH, EVENT_PROPERTY_PAGE_REFERER, EVENT_PROPERTY_PAGE_SEARCH, EVENT_PROPERTY_PAGE_TITLE, EVENT_PROPERTY_PAGE_URL, EVENT_SCREEN, TABLE_EVENTS, USER_PROPERTY_CLIENT_FAMILY, USER_PROPERTY_CLIENT_VERSION_MAJOR, USER_PROPERTY_CLIENT_VERSION_MINOR, USER_PROPERTY_CLIENT_VERSION_PATCH, USER_PROPERTY_DEVICE_BRAND, USER_PROPERTY_DEVICE_FAMILY, USER_PROPERTY_DEVICE_MODEL, USER_PROPERTY_OS_FAMILY, USER_PROPERTY_OS_VERSION_MAJOR, USER_PROPERTY_OS_VERSION_MINOR, USER_PROPERTY_OS_VERSION_PATCH, USER_PROPERTY_OS_VERSION_PATCH_MINOR};
use metadata::accounts::CreateAccountRequest;
use metadata::MetadataProvider;
use metadata::organizations::CreateOrganizationRequest;
use metadata::projects::CreateProjectRequest;
use metadata::properties::{DictionaryType, Type};
use platform::auth::password::make_password_hash;
use test_util::{create_event, create_property, CreatePropertyMainRequest};

pub mod error;
pub mod store;
pub mod test;


pub fn init_system(md: &Arc<MetadataProvider>, db: &Arc<OptiDBImpl>) -> error::Result<()> {
    db.create_table(TABLE_EVENTS, TableOptions::test())?;
    create_property(
        md,
        0,
        0,
        CreatePropertyMainRequest {
            name: COLUMN_PROJECT_ID.to_string(),
            typ: Type::System,
            data_type: DType::Int64,
            nullable: false,
            dict: None,
        },
    )?;

    create_property(
        md,
        0,
        0,
        CreatePropertyMainRequest {
            name: COLUMN_USER_ID.to_string(),
            typ: Type::System,
            data_type: DType::Int64,
            nullable: false,
            dict: None,
        },
    )?;

    create_property(
        md,
        0,
        0,
        CreatePropertyMainRequest {
            name: COLUMN_CREATED_AT.to_string(),
            typ: Type::System,
            data_type: DType::Timestamp,
            nullable: false,
            dict: None,
        },
    )?;

    create_property(
        md,
        0,
        0,
        CreatePropertyMainRequest {
            name: COLUMN_EVENT_ID.to_string(),
            typ: Type::System,
            data_type: DType::Int64,
            nullable: false,
            dict: None,
        },
    )?;

    create_property(
        md,
        0,
        0,
        CreatePropertyMainRequest {
            name: COLUMN_EVENT.to_string(),
            typ: Type::System,
            data_type: DType::String,
            nullable: false,
            dict: Some(DictionaryType::Int64),
        },
    )?;


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
        USER_PROPERTY_OS_FAMILY,
        USER_PROPERTY_OS_VERSION_MAJOR,
        USER_PROPERTY_OS_VERSION_MINOR,
        USER_PROPERTY_OS_VERSION_PATCH,
        USER_PROPERTY_OS_VERSION_PATCH_MINOR,
    ];
    for prop in user_props {
        create_property(
            md,
            org_id, project_id,
            CreatePropertyMainRequest {
                name: prop.to_string(),
                typ: Type::User,
                data_type: DType::String,
                nullable: true,
                dict: Some(DictionaryType::Int64),
            },
        )?;
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
        create_property(
            md,
            org_id, project_id,
            CreatePropertyMainRequest {
                name: prop.to_string(),
                typ: Type::Event,
                data_type: DType::String,
                nullable: true,
                dict: None,
            },
        )?;
    }

    Ok(())
}

fn init_test_org_structure(md: &Arc<MetadataProvider>) -> crate::error::Result<(u64, u64)> {
    let admin = match md.accounts.create(CreateAccountRequest {
        created_by: None,
        password_hash: make_password_hash("admin")?,
        email: "admin@email.com".to_string(),
        first_name: Some("admin".to_string()),
        last_name: None,
        role: Some(Role::Admin),
        organizations: None,
        projects: None,
        teams: None,
    }) {
        Ok(acc) => acc,
        Err(err) => md.accounts.get_by_email("admin@email.com")?,
    };
    let org = match md.organizations.create(CreateOrganizationRequest {
        created_by: admin.id,
        name: "Test Organization".to_string(),
    }) {
        Ok(org) => org,
        Err(err) => md.organizations.get_by_id(1)?,
    };

    let proj = match md.projects.create(org.id, CreateProjectRequest {
        created_by: admin.id,
        name: "Test Project".to_string(),
    }) {
        Ok(proj) => proj,
        Err(err) => md.projects.get_by_id(1, 1)?,
    };

    info!("token: {}",proj.token);
    let _user = match md.accounts.create(CreateAccountRequest {
        created_by: Some(admin.id),
        password_hash: make_password_hash("test")?,
        email: "user@test.com".to_string(),
        first_name: Some("user".to_string()),
        last_name: None,
        role: None,
        organizations: Some(vec![(org.id, OrganizationRole::Member)]),
        projects: Some(vec![(proj.id, ProjectRole::Reader)]),
        teams: None,
    }) {
        Ok(acc) => acc,
        Err(err) => md.accounts.get_by_email("user@test.com")?,
    };

    Ok((org.id, proj.id))
}
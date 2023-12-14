use std::sync::Arc;
use ::store::db::OptiDBImpl;
use common::types::{COLUMN_CREATED_AT, COLUMN_EVENT, COLUMN_EVENT_ID, COLUMN_PROJECT_ID, COLUMN_USER_ID, DType, EVENT_CLICK, EVENT_PAGE, EVENT_PROPERTY_A_CLASS, EVENT_PROPERTY_A_HREF, EVENT_PROPERTY_A_ID, EVENT_PROPERTY_A_NAME, EVENT_PROPERTY_A_STYLE, EVENT_PROPERTY_PAGE_PATH, EVENT_PROPERTY_PAGE_REFERER, EVENT_PROPERTY_PAGE_SEARCH, EVENT_PROPERTY_PAGE_TITLE, EVENT_PROPERTY_PAGE_URL, EVENT_SCREEN, USER_PROPERTY_CLIENT_FAMILY, USER_PROPERTY_CLIENT_VERSION_MAJOR, USER_PROPERTY_CLIENT_VERSION_MINOR, USER_PROPERTY_CLIENT_VERSION_PATCH, USER_PROPERTY_DEVICE_BRAND, USER_PROPERTY_DEVICE_FAMILY, USER_PROPERTY_DEVICE_MODEL, USER_PROPERTY_OS_FAMILY, USER_PROPERTY_OS_VERSION_MAJOR, USER_PROPERTY_OS_VERSION_MINOR, USER_PROPERTY_OS_VERSION_PATCH, USER_PROPERTY_OS_VERSION_PATCH_MINOR};
use metadata::MetadataProvider;
use metadata::properties::{DictionaryType, Type};
use test_util::{create_event, create_property, CreatePropertyMainRequest};

pub mod error;
pub mod store;
pub mod test;


pub fn init_db(md:&Arc<MetadataProvider>) -> error::Result<()> {
    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: COLUMN_PROJECT_ID.to_string(),
            typ: Type::Event,
            data_type: DType::Int64,
            nullable: false,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: COLUMN_USER_ID.to_string(),
            typ: Type::Event,
            data_type: DType::Int64,
            nullable: false,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: COLUMN_CREATED_AT.to_string(),
            typ: Type::Event,
            data_type: DType::Timestamp,
            nullable: false,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: COLUMN_EVENT_ID.to_string(),
            typ: Type::Event,
            data_type: DType::Int64,
            nullable: false,
            dict: None,
        },
        &db,
    )?;

    create_property(
        md,
        org_id,
        proj_id,
        CreatePropertyMainRequest {
            name: COLUMN_EVENT.to_string(),
            typ: Type::Event,
            data_type: DType::String,
            nullable: false,
            dict: Some(DictionaryType::Int64),
        },
        &db,
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
            }
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
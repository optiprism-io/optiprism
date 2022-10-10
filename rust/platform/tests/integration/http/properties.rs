use axum::http::HeaderValue;
use axum::{Router, Server};
use chrono::Utc;
use metadata::metadata::ListResponse;
use metadata::properties::{CreatePropertyRequest, Provider};
use metadata::store::Store;
use platform::error::Result;
use platform::http::properties;
use platform::properties::{Property, Provider as PropertiesProvider, Status, UpdatePropertyRequest};
use reqwest::header::HeaderMap;
use reqwest::{Client, StatusCode};
use std::env::temp_dir;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use uuid::Uuid;
use common::DataType;
use common::types::{DictionaryDataType, OptionalProperty};
use crate::http::tests::{create_admin_acc_and_login, run_http_service};

fn assert(l: &Property, r: &Property) {
    assert_eq!(l.id, r.id);
    assert_eq!(l.project_id, r.project_id);
    assert_eq!(l.tags, r.tags);
    assert_eq!(l.name, r.name);
    assert_eq!(l.display_name, r.display_name);
    assert_eq!(l.description, r.description);
    assert_eq!(l.status, r.status);
    assert_eq!(l.typ, r.typ);
    assert_eq!(l.nullable, r.nullable);
    assert_eq!(l.is_array, r.is_dictionary);
    assert_eq!(l.dictionary_type, r.dictionary_type);
}

#[tokio::test]
async fn test_event_properties() -> Result<()> {
    let (md, pp) = run_http_service(false).await?;
    let cl = Client::new();
    let headers = create_admin_acc_and_login(&pp.auth, &md.accounts, &cl).await?;

    let mut prop1 = Property {
        id: 1,
        created_at: Utc::now(),
        updated_at: None,
        created_by: 0,
        updated_by: None,
        project_id: 1,
        tags: Some(vec!["sdf".to_string()]),
        name: "qwe".to_string(),
        display_name: Some("dname".to_string()),
        typ: DataType::String,
        description: Some("desc".to_string()),
        status: Status::Enabled,
        nullable: true,
        is_array: true,
        is_dictionary: true,
        dictionary_type: Some(DictionaryDataType::UInt8),
        is_system: false,
    };

    // list without props should be empty
    {
        cl.get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/event_properties")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
    }

    // get of unexisting event prop 1 should return 404 not found error
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/event_properties/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // delete of unexisting event prop 1 should return 404 not found error
    {
        let resp = cl
            .delete("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/event_properties/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // create request should create event prop
    {
        let req = CreatePropertyRequest {
            created_by: prop1.created_by.clone(),
            tags: prop1.tags.clone(),
            name: prop1.name.clone(),
            description: prop1.description.clone(),
            display_name: prop1.display_name.clone(),
            typ: prop1.typ.clone().try_into()?,
            status: prop1.status.clone().into(),
            nullable: prop1.nullable.clone(),
            is_array: prop1.is_array.clone(),
            is_dictionary: prop1.is_dictionary.clone(),
            dictionary_type: prop1.dictionary_type.clone().map(|v|v.try_into()).transpose()?,
            is_system: false,
        };

        let resp = md.event_properties.create(1, 1, req).await?;
        assert_eq!(resp.id, 1);
    }

    // update request should update prop
    {
        prop1.tags = Some(vec!["ert".to_string()]);
        prop1.display_name = Some("ert".to_string());
        prop1.description = Some("xcv".to_string());
        prop1.status = Status::Disabled;

        let req = UpdatePropertyRequest {
            tags: OptionalProperty::Some(prop1.tags.clone()),
            display_name: OptionalProperty::Some(prop1.display_name.clone()),
            description: OptionalProperty::Some(prop1.description.clone()),
            status: OptionalProperty::Some(prop1.status.clone()),
        };

        let body = serde_json::to_string(&req).unwrap();

        let resp = cl
            .put("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/event_properties/1")
            .body(body)
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let p: Property = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&p, &prop1)
    }

    // get should return event prop
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/event_properties/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let p: Property = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&p, &prop1)
    }
    // list events should return list with one event
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/event_properties")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let resp: ListResponse<Property> =
            serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(resp.data.len(), 1);
        assert(&resp.data[0], &prop1);
    }

    // delete request should delete event
    {
        let resp = cl
            .delete("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/event_properties/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = cl
            .delete("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/event_properties/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
    Ok(())
}

use std::env::temp_dir;
use std::net::SocketAddr;
use std::sync::Arc;
use axum::{AddExtensionLayer, Router, Server};
use axum::http::HeaderValue;
use chrono::Utc;
use reqwest::{Client, StatusCode};
use reqwest::header::HeaderMap;
use uuid::Uuid;
use metadata::{Metadata, Store};
use platform::events::Provider as EventsProvider;
use platform::error::Result;
use tokio::time::{sleep, Duration};
use metadata::events::{Event, Scope, Status};
use metadata::metadata::ListResponse;
use platform::events::{CreateRequest, UpdateRequest};
use platform::http::events;

#[tokio::test]
async fn test_events() -> Result<()> {
    tokio::spawn(async {
        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));
        let store = Arc::new(Store::new(path));
        let metadata = Arc::new(Metadata::try_new(store).unwrap());
        let events_provider = Arc::new(EventsProvider::new(metadata.clone()));

        let app =
            events::configure(Router::new())
                .layer(AddExtensionLayer::new(events_provider));

        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        Server::bind(&addr)
            .serve(app.into_make_service())
            .await.unwrap();
    });


    sleep(Duration::from_millis(100)).await;

    let mut event1 = Event {
        id: 1,
        created_at: Utc::now(),
        updated_at: None,
        created_by: 0,
        updated_by: None,
        project_id: 1,
        tags: Some(vec!["sdf".to_string()]),
        name: "qwe".to_string(),
        display_name: Some("dname".to_string()),
        description: Some("desc".to_string()),
        status: Status::Enabled,
        scope: Scope::System,
        properties: Some(vec![1]),
        custom_properties: Some(vec![3]),
    };

    let cl = Client::new();
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_str("application/json").unwrap());
    // list without events should be empty
    {
        let resp = cl.get("http://127.0.0.1:8080/v1/projects/1/events").headers(headers.clone()).send().await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.text().await.unwrap(), r#"{"data":[],"meta":{"next":null}}"#);
    }

    // get of unexisting event 1 should return 404 not found error
    {
        let resp = cl.get("http://127.0.0.1:8080/v1/projects/1/events/1").headers(headers.clone()).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // delete of unexisting event 1 should return 404 not found error
    {
        let resp = cl.delete("http://127.0.0.1:8080/v1/projects/1/events/1").headers(headers.clone()).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // create request should create event
    {
        let req = CreateRequest {
            project_id: 1,
            tags: event1.tags.clone(),
            name: event1.name.clone(),
            display_name: event1.display_name.clone(),
            description: event1.description.clone(),
            status: event1.status.clone(),
            scope: event1.scope.clone(),
            properties: event1.properties.clone(),
            custom_properties: event1.custom_properties.clone(),
        };


        let body = serde_json::to_string(&req).unwrap();

        let resp = cl.post("http://127.0.0.1:8080/v1/projects/1/events").body(body).headers(headers.clone()).send().await.unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let resp: Event = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(resp.id, 1);
        assert_eq!(resp.project_id, event1.project_id);
        assert_eq!(resp.tags, event1.tags);
        assert_eq!(resp.name, event1.name);
        assert_eq!(resp.display_name, event1.display_name);
        assert_eq!(resp.description, event1.description);
        assert_eq!(resp.status, event1.status);
        assert_eq!(resp.scope, event1.scope);
        assert_eq!(resp.properties, event1.properties);
        assert_eq!(resp.custom_properties, event1.custom_properties);
    }

    // update request should update event
    {
        event1.tags = Some(vec!["ert".to_string()]);
        event1.name = "cxb".to_string();
        event1.display_name = Some("ert".to_string());
        event1.description = Some("xcv".to_string());
        event1.status = Status::Disabled;
        event1.scope = Scope::User;
        event1.properties = Some(vec![2]);
        event1.custom_properties = Some(vec![4]);

        let req = UpdateRequest {
            id: 1,
            project_id: 1,
            tags: event1.tags.clone(),
            name: event1.name.clone(),
            display_name: event1.display_name.clone(),
            description: event1.description.clone(),
            status: event1.status.clone(),
            scope: event1.scope.clone(),
            properties: event1.properties.clone(),
            custom_properties: event1.custom_properties.clone(),
        };


        let body = serde_json::to_string(&req).unwrap();

        let resp = cl.put("http://127.0.0.1:8080/v1/projects/1/events/1").body(body).headers(headers.clone()).send().await.unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let e: Event = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(e.id, 1);
        assert_eq!(e.project_id, event1.project_id);
        assert_eq!(e.tags, event1.tags);
        assert_eq!(e.name, event1.name);
        assert_eq!(e.display_name, event1.display_name);
        assert_eq!(e.description, event1.description);
        assert_eq!(e.status, event1.status);
        assert_eq!(e.scope, event1.scope);
        assert_eq!(e.properties, event1.properties);
        assert_eq!(e.custom_properties, event1.custom_properties);
    }

    // get should return event
    {
        let resp = cl.get("http://127.0.0.1:8080/v1/projects/1/events/1").headers(headers.clone()).send().await.unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let e: Event = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(e.id, 1);
        assert_eq!(e.project_id, event1.project_id);
        assert_eq!(e.tags, event1.tags);
        assert_eq!(e.name, event1.name);
        assert_eq!(e.display_name, event1.display_name);
        assert_eq!(e.description, event1.description);
        assert_eq!(e.status, event1.status);
        assert_eq!(e.scope, event1.scope);
        assert_eq!(e.properties, event1.properties);
        assert_eq!(e.custom_properties, event1.custom_properties);
    }
    // list events should return list with one event
    {
        let resp = cl.get("http://127.0.0.1:8080/v1/projects/1/events").headers(headers.clone()).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let resp: ListResponse<Event> = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(resp.data.len(),1);
        assert_eq!(resp.data[0].id, 1);
        assert_eq!(resp.data[0].project_id, event1.project_id);
        assert_eq!(resp.data[0].tags, event1.tags);
        assert_eq!(resp.data[0].name, event1.name);
        assert_eq!(resp.data[0].display_name, event1.display_name);
        assert_eq!(resp.data[0].description, event1.description);
        assert_eq!(resp.data[0].status, event1.status);
        assert_eq!(resp.data[0].scope, event1.scope);
        assert_eq!(resp.data[0].properties, event1.properties);
        assert_eq!(resp.data[0].custom_properties, event1.custom_properties);
    }

    // delete request should delete event
    {
        let resp = cl.delete("http://127.0.0.1:8080/v1/projects/1/events/1").headers(headers.clone()).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = cl.delete("http://127.0.0.1:8080/v1/projects/1/events/1").headers(headers.clone()).send().await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
    Ok(())
}
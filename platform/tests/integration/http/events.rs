use axum::http::HeaderValue;
use axum::{AddExtensionLayer, Router, Server};
use chrono::Utc;
use metadata::events::{Event, Provider, Status};
use metadata::metadata::ListResponse;
use metadata::Store;
use platform::error::Result;
use platform::events::Provider as EventsProvider;
use platform::events::{CreateRequest, UpdateRequest};
use platform::http::events;
use reqwest::header::HeaderMap;
use reqwest::{Client, StatusCode};
use std::env::temp_dir;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

fn assert(l: &Event, r: &Event) {
    assert_eq!(l.id, 1);
    assert_eq!(l.project_id, r.project_id);
    assert_eq!(l.tags, r.tags);
    assert_eq!(l.name, r.name);
    assert_eq!(l.display_name, r.display_name);
    assert_eq!(l.description, r.description);
    assert_eq!(l.status, r.status);
    assert_eq!(l.properties, r.properties);
    assert_eq!(l.custom_properties, r.custom_properties);
}

#[tokio::test]
async fn test_events() -> Result<()> {
    tokio::spawn(async {
        let mut path = temp_dir();
        path.push(format!("{}.db", Uuid::new_v4()));
        let store = Arc::new(Store::new(path));
        let prov = Provider::new(store);
        let events_provider = Arc::new(EventsProvider::new(Arc::new(prov)));

        let app = events::attach_routes(Router::new(), events_provider);

        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
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
        properties: None,
        custom_properties: None,
        is_system: false
    };

    let cl = Client::new();
    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Type",
        HeaderValue::from_str("application/json").unwrap(),
    );
    // list without events should be empty
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/events")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.text().await.unwrap(),
            r#"{"data":[],"meta":{"next":null}}"#
        );
    }

    // get of unexisting event 1 should return 404 not found error
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/events/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // delete of unexisting event 1 should return 404 not found error
    {
        let resp = cl
            .delete("http://127.0.0.1:8080/v1/organizations/1/projects/1/events/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // create request should create event
    {
        let req = CreateRequest {
            tags: event1.tags.clone(),
            name: event1.name.clone(),
            display_name: event1.display_name.clone(),
            description: event1.description.clone(),
            status: event1.status.clone(),
            is_system: false
        };

        let body = serde_json::to_string(&req).unwrap();

        let resp = cl
            .post("http://127.0.0.1:8080/v1/organizations/1/projects/1/events")
            .body(body)
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let resp: Event = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&resp, &event1)
    }

    // update request should update event
    {
        event1.tags = Some(vec!["ert".to_string()]);
        event1.display_name = Some("ert".to_string());
        event1.description = Some("xcv".to_string());
        event1.status = Status::Disabled;

        let req = UpdateRequest {
            tags: event1.tags.clone(),
            display_name: event1.display_name.clone(),
            description: event1.description.clone(),
            status: event1.status.clone(),
        };

        let body = serde_json::to_string(&req).unwrap();

        let resp = cl
            .put("http://127.0.0.1:8080/v1/organizations/1/projects/1/events/1")
            .body(body)
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let e: Event = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(e.id, 1);
        assert_eq!(e.project_id, event1.project_id);
        assert_eq!(e.tags, event1.tags);
        assert_eq!(e.display_name, event1.display_name);
        assert_eq!(e.description, event1.description);
        assert_eq!(e.status, event1.status);
        assert_eq!(e.properties, event1.properties);
        assert_eq!(e.custom_properties, event1.custom_properties);
    }

    // get should return event
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/events/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let e: Event = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&e, &event1);
    }
    // list events should return list with one event
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/events")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let resp: ListResponse<Event> =
            serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(resp.data.len(), 1);
        assert(&resp.data[0], &event1);
    }

    // delete request should delete event
    {
        let resp = cl
            .delete("http://127.0.0.1:8080/v1/organizations/1/projects/1/events/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = cl
            .delete("http://127.0.0.1:8080/v1/organizations/1/projects/1/events/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
    Ok(())
}

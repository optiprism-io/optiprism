use axum::http::HeaderValue;
use axum::{Router, Server};
use chrono::Utc;
use metadata::metadata::ListResponse;
use metadata::store::Store;
use platform::error::Result;

use metadata::custom_events::Provider;
use platform::custom_events::types::{
    CreateCustomEventRequest, CustomEvent, Event, Status, UpdateCustomEventRequest,
};
use platform::http::custom_events;
use platform::queries::types::EventRef;
use platform::CustomEventsProvider;
use reqwest::header::HeaderMap;
use reqwest::{Client, StatusCode};
use std::env::temp_dir;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use uuid::Uuid;

fn assert(l: &CustomEvent, r: &CustomEvent) {
    assert_eq!(l.id, 1);
    assert_eq!(l.project_id, r.project_id);
    assert_eq!(l.tags, r.tags);
    assert_eq!(l.name, r.name);
    assert_eq!(l.description, r.description);
    assert_eq!(l.status, r.status);
    assert_eq!(l.events, r.events);
}

#[tokio::test]
async fn test_custom_events() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    let store = Arc::new(Store::new(path));
    let md_events = Arc::new(metadata::events::Provider::new(store.clone()));
    let md_custom_events = Arc::new(Provider::new(store.clone(), md_events.clone()));
    let custom_events_prov = Arc::new(CustomEventsProvider::new(md_custom_events));
    tokio::spawn(async {
        let app = custom_events::attach_routes(Router::new(), custom_events_prov);
        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        Server::bind(&addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(100)).await;

    let event1 = md_events
        .create(
            1,
            1,
            metadata::events::CreateEventRequest {
                created_by: 0,
                tags: None,
                name: "e1".to_string(),
                display_name: None,
                description: None,
                status: metadata::events::Status::Enabled,
                is_system: false,
                properties: None,
                custom_properties: None,
            },
        )
        .await?;

    let event2 = md_events
        .create(
            1,
            1,
            metadata::events::CreateEventRequest {
                created_by: 0,
                tags: None,
                name: "e2".to_string(),
                display_name: None,
                description: None,
                status: metadata::events::Status::Enabled,
                is_system: false,
                properties: None,
                custom_properties: None,
            },
        )
        .await?;

    let mut ce1 = CustomEvent {
        id: 1,
        created_at: Utc::now(),
        updated_at: None,
        created_by: 0,
        updated_by: None,
        project_id: 1,
        tags: Some(vec!["sdf".to_string()]),
        name: "qwe".to_string(),
        description: Some("desc".to_string()),
        status: Status::Enabled,
        is_system: false,
        events: vec![Event {
            event: EventRef::Regular {
                event_name: event1.name.clone(),
            },
            filters: None,
        }],
    };

    println!("{:?}\n", ce1);
    let cl = Client::new();
    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Type",
        HeaderValue::from_str("application/json").unwrap(),
    );
    // list without events should be empty
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/custom-events")
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
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/custom-events/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // delete of unexisting event 1 should return 404 not found error
    {
        let resp = cl
            .delete("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/custom-events/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // create request should create event
    {
        let req = CreateCustomEventRequest {
            tags: ce1.tags.clone(),
            name: ce1.name.clone(),
            description: ce1.description.clone(),
            status: ce1.status.clone(),
            is_system: false,
            events: ce1.events.clone(),
        };
        let body = serde_json::to_string(&req).unwrap();

        let resp = cl
            .post("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/custom-events")
            .body(body)
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let resp: CustomEvent = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        println!("resp {:?}\n", resp);
        assert(&resp, &ce1)
    }

    // update request should update event
    {
        ce1.tags = Some(vec!["ert".to_string()]);
        ce1.description = Some("xcv".to_string());
        ce1.status = Status::Disabled;
        ce1.events = vec![Event {
            event: EventRef::Regular {
                event_name: event2.name.clone(),
            },
            filters: None,
        }];

        let req = UpdateCustomEventRequest {
            tags: Some(ce1.tags.clone()),
            name: Some(ce1.name.clone()),
            description: Some(ce1.description.clone()),
            status: Some(ce1.status.clone()),
            is_system: None,
            events: Some(vec![Event {
                event: EventRef::Regular {
                    event_name: event2.name,
                },
                filters: None,
            }]),
        };

        let body = serde_json::to_string(&req).unwrap();

        let resp = cl
            .put("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/custom-events/1")
            .body(body)
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let e: CustomEvent = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();

        assert(&e, &ce1);
    }

    // get should return event
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/custom-events/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let e: CustomEvent = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&e, &ce1);
    }
    // list events should return list with one event
    {
        let resp = cl
            .get("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/custom-events")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let resp: ListResponse<CustomEvent> =
            serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(resp.data.len(), 1);
        assert(&resp.data[0], &ce1);
    }

    // delete request should delete event
    {
        let resp = cl
            .delete("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/custom-events/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = cl
            .delete("http://127.0.0.1:8080/v1/organizations/1/projects/1/schema/events/1")
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
    Ok(())
}

use chrono::Utc;
use common::types::OptionalProperty;
use platform::custom_events::CreateCustomEventRequest;
use platform::custom_events::CustomEvent;
use platform::custom_events::Event;
use platform::custom_events::Status;
use platform::custom_events::UpdateCustomEventRequest;
use platform::EventRef;
use platform::ListResponse;
use reqwest::Client;
use reqwest::StatusCode;

use crate::assert_response_json_eq;
use crate::assert_response_status_eq;
use crate::http::tests::create_admin_acc_and_login;
use crate::http::tests::run_http_service;
use crate::http::tests::EMPTY_LIST;

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
async fn test_custom_events() {
    let (base_url, md, pp) = run_http_service(false).await.unwrap();
    let events_url = format!("{base_url}/projects/1/schema/custom-events");
    let cl = Client::new();
    let admin_headers = create_admin_acc_and_login(&pp.auth, &md.accounts)
        .await
        .unwrap();

    let event1 = md
        .events
        .create(1, metadata::events::CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "e1".to_string(),
            display_name: None,
            description: None,
            status: metadata::events::Status::Enabled,
            is_system: false,
            event_properties: None,
            user_properties: None,
            custom_properties: None,
        })
        .unwrap();

    let event2 = md
        .events
        .create(1, metadata::events::CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "e2".to_string(),
            display_name: None,
            description: None,
            status: metadata::events::Status::Enabled,
            is_system: false,
            event_properties: None,
            user_properties: None,
            custom_properties: None,
        })
        .unwrap();

    let mut custom_event1 = CustomEvent {
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

    // list without events should be empty
    {
        let resp = cl
            .get(&events_url)
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::OK);
        assert_response_json_eq!(resp, EMPTY_LIST.to_string());
    }

    // get of unexisting event 1 should return 404 not found error
    {
        let resp = cl
            .get(format!("{events_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::NOT_FOUND);
    }

    // delete of unexisting event 1 should return 404 not found error
    {
        let resp = cl
            .delete(format!("{events_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();

        assert_response_status_eq!(resp, StatusCode::NOT_FOUND);
    }

    // create request should create event
    {
        let req = CreateCustomEventRequest {
            tags: custom_event1.tags.clone(),
            name: custom_event1.name.clone(),
            description: custom_event1.description.clone(),
            status: custom_event1.status.clone(),
            is_system: false,
            events: custom_event1.events.clone(),
        };

        let resp = cl
            .post(&events_url)
            .body(serde_json::to_string(&req).unwrap())
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::CREATED);
        let resp: CustomEvent = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&resp, &custom_event1)
    }

    // update request should update event
    {
        custom_event1.tags = Some(vec!["ert".to_string()]);
        custom_event1.description = Some("xcv".to_string());
        custom_event1.status = Status::Disabled;
        custom_event1.events = vec![Event {
            event: EventRef::Regular {
                event_name: event2.name.clone(),
            },
            filters: None,
        }];

        let req = UpdateCustomEventRequest {
            tags: OptionalProperty::Some(custom_event1.tags.clone()),
            name: OptionalProperty::Some(custom_event1.name.clone()),
            description: OptionalProperty::Some(custom_event1.description.clone()),
            status: OptionalProperty::Some(custom_event1.status.clone()),
            events: OptionalProperty::Some(vec![Event {
                event: EventRef::Regular {
                    event_name: event2.name,
                },
                filters: None,
            }]),
        };

        let resp = cl
            .put(format!("{events_url}/1"))
            .body(serde_json::to_string(&req).unwrap())
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::OK);
        let e: CustomEvent = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();

        assert(&e, &custom_event1);
    }

    // get should return event
    {
        let resp = cl
            .get(format!("{events_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::OK);
        let e: CustomEvent = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&e, &custom_event1);
    }
    // list events should return list with one event
    {
        let resp = cl
            .get(&events_url)
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::OK);
        let resp: ListResponse<CustomEvent> =
            serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert_eq!(resp.len(), 1);
        assert(&resp.data[0], &custom_event1);
    }

    // delete request should delete event
    {
        let resp = cl
            .delete(format!("{events_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::OK);

        let resp = cl
            .delete(format!("{events_url}/1"))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_response_status_eq!(resp, StatusCode::NOT_FOUND);
    }
}

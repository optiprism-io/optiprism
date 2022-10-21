use chrono::Utc;
use metadata::metadata::ListResponse;

use platform::error::Result;

use platform::custom_events::types::{
    CreateCustomEventRequest, CustomEvent, Event, Status, UpdateCustomEventRequest,
};

use platform::queries::types::EventRef;

use reqwest::{Client, StatusCode};

use crate::http::tests::{create_admin_acc_and_login, run_http_service};
use common::types::OptionalProperty;

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
    let (base_url, md, pp) = run_http_service(false).await?;
    println!("{base_url}");
    let cl = Client::new();
    let admin_headers = create_admin_acc_and_login(&pp.auth, &md.accounts).await?;

    let event1 = md
        .events
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

    let event2 = md
        .events
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

    // list without events should be empty
    {
        let resp = cl
            .get(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/custom-events"
            ))
            .headers(admin_headers.clone())
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
            .get(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/custom-events/1"
            ))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // delete of unexisting event 1 should return 404 not found error
    {
        let resp = cl
            .delete(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/custom-events/1"
            ))
            .headers(admin_headers.clone())
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
            .post(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/custom-events"
            ))
            .body(body)
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::CREATED);
        let resp: CustomEvent = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
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
            tags: OptionalProperty::Some(ce1.tags.clone()),
            name: OptionalProperty::Some(ce1.name.clone()),
            description: OptionalProperty::Some(ce1.description.clone()),
            status: OptionalProperty::Some(ce1.status.clone()),
            events: OptionalProperty::Some(vec![Event {
                event: EventRef::Regular {
                    event_name: event2.name,
                },
                filters: None,
            }]),
        };

        let body = serde_json::to_string(&req).unwrap();

        println!("{body}");

        let resp = cl
            .put(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/custom-events/1"
            ))
            .body(body)
            .headers(admin_headers.clone())
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
            .get(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/custom-events/1"
            ))
            .headers(admin_headers.clone())
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
            .get(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/custom-events"
            ))
            .headers(admin_headers.clone())
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
            .delete(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/custom-events/1"
            ))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = cl
            .delete(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/custom-events/1"
            ))
            .headers(admin_headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
    Ok(())
}

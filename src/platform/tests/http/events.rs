use chrono::Utc;
use common::types::OptionalProperty;
use metadata::metadata::ListResponse;
use platform::error::Result;
use platform::events::CreateEventRequest;
use platform::events::Event;
use platform::events::Status;
use platform::events::UpdateEventRequest;
use reqwest::Client;
use reqwest::StatusCode;

use crate::http::tests::create_admin_acc_and_login;
use crate::http::tests::run_http_service;

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
    let (base_url, md, pp) = run_http_service(false).await?;
    let cl = Client::new();
    let headers = create_admin_acc_and_login(&pp.auth, &md.accounts).await?;

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
        is_system: false,
    };

    // list without events should be empty
    {
        let resp = cl
            .get(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/events"
            ))
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
            .get(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/events/1"
            ))
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // delete of unexisting event 1 should return 404 not found error
    {
        let resp = cl
            .delete(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/events/1"
            ))
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // create request should create event
    {
        let req = CreateEventRequest {
            tags: event1.tags.clone(),
            name: event1.name.clone(),
            display_name: event1.display_name.clone(),
            description: event1.description.clone(),
            status: event1.status.clone(),
            is_system: false,
        };

        let body = serde_json::to_string(&req).unwrap();

        let resp = cl
            .post(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/events"
            ))
            .body(body)
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::CREATED);
        let resp: Event = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&resp, &event1)
    }

    // update request should update event
    {
        event1.tags = Some(vec!["ert".to_string()]);
        event1.display_name = Some("ert".to_string());
        event1.description = Some("xcv".to_string());
        event1.status = Status::Disabled;

        let req = UpdateEventRequest {
            tags: OptionalProperty::Some(event1.tags.clone()),
            display_name: OptionalProperty::Some(event1.display_name.clone()),
            description: OptionalProperty::Some(event1.description.clone()),
            status: OptionalProperty::Some(event1.status.clone()),
        };

        let body = serde_json::to_string(&req).unwrap();

        let resp = cl
            .put(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/events/1"
            ))
            .body(body)
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        let status = resp.status();
        assert_eq!(status, StatusCode::OK);
        let e: Event = serde_json::from_str(resp.text().await.unwrap().as_str()).unwrap();
        assert(&e, &event1);
    }

    // get should return event
    {
        let resp = cl
            .get(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/events/1"
            ))
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
            .get(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/events"
            ))
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
            .delete(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/events/1"
            ))
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let resp = cl
            .delete(format!(
                "{base_url}/api/v1/organizations/1/projects/1/schema/events/1"
            ))
            .headers(headers.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
    Ok(())
}

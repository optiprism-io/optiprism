use metadata::error::{CustomEventError, MetadataError, Result};
use metadata::store::Store;
use std::env::temp_dir;
use std::sync::Arc;

use metadata::events::types::CreateEventRequest;
use metadata::custom_events::{CreateCustomEventRequest, Provider, Status, UpdateCustomEventRequest};
use uuid::Uuid;
use metadata::custom_events::types::{Event, EventType};
use metadata::events;

fn get_providers() -> (Arc<events::Provider>, Provider) {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    let store = Arc::new(Store::new(path));
    let events_prov = Arc::new(events::Provider::new(store.clone()));
    let custom_events = Provider::new(store.clone(), events_prov.clone());

    (events_prov, custom_events)
}

#[tokio::test]
async fn non_exist() -> Result<()> {
    let (_, custom_events) = get_providers();

    // try to get, delete, update unexisting event
    assert!(custom_events.get_by_id(1, 1, 1).await.is_err());
    assert!(custom_events.delete(1, 1, 1).await.is_err());

    let update_event_req = UpdateCustomEventRequest {
        updated_by: 1,
        tags: None,
        name: None,
        description: None,
        status: None,
        is_system: None,
        events: None,
    };

    assert!(custom_events
        .update(1, 1, 1, update_event_req.clone())
        .await
        .is_err());
    Ok(())
}

#[tokio::test]
async fn create_event() -> Result<()> {
    let (event_prov, prov) = get_providers();

    let event = event_prov.create(1, 1, CreateEventRequest {
        created_by: 0,
        tags: None,
        name: "e1".to_string(),
        display_name: None,
        description: None,
        status: events::Status::Enabled,
        is_system: false,
        properties: None,
        custom_properties: None,
    }).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "some name".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event { id: event.id, typ: EventType::Regular }],
    };

    let resp = prov.create(1, 1, req.clone()).await?;
    assert_eq!(resp.name, req.name);
    let check = prov.get_by_id(1, 1, resp.id).await?;
    assert_eq!(resp, check);
    Ok(())
}

#[tokio::test]
async fn create_event_not_found() -> Result<()> {
    let (event_prov, prov) = get_providers();

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "some name".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event { id: 1, typ: EventType::Regular }],
    };

    let resp = prov.create(1, 1, req.clone()).await;
    assert!(resp.is_err());
    Ok(())
}

#[tokio::test]
async fn create_event_duplicate_name() -> Result<()> {
    let (event_prov, prov) = get_providers();

    event_prov.create(1, 1, CreateEventRequest {
        created_by: 0,
        tags: None,
        name: "e1".to_string(),
        display_name: None,
        description: None,
        status: events::Status::Enabled,
        is_system: false,
        properties: None,
        custom_properties: None,
    }).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "some name".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event { id: 1, typ: EventType::Regular }],
    };

    let resp = prov.create(1, 1, req.clone()).await;
    assert!(resp.is_ok());
    let resp = prov.create(1, 1, req.clone()).await;
    assert!(resp.is_err());
    Ok(())
}

#[tokio::test]
async fn create_event_recursion_level_exceeded() -> Result<()> {
    let (event_prov, prov) = get_providers();
    let prov = prov.with_max_events_level(1);

    event_prov.create(1, 1, CreateEventRequest {
        created_by: 0,
        tags: None,
        name: "e1".to_string(),
        display_name: None,
        description: None,
        status: events::Status::Enabled,
        is_system: false,
        properties: None,
        custom_properties: None,
    }).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "1".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event { id: 1, typ: EventType::Regular }],
    };
    let resp = prov.create(1, 1, req.clone()).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "2".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event { id: resp.id, typ: EventType::Custom }],
    };
    let resp = prov.create(1, 1, req.clone()).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "3".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event { id: resp.id, typ: EventType::Custom }],
    };
    let resp = prov.create(1, 1, req.clone()).await;
    assert!(matches!(resp,Err(MetadataError::CustomEvent(CustomEventError::RecursionLevelExceeded(_)))));
    Ok(())
}

#[tokio::test]
async fn test_duplicate() -> Result<()> {
    let (event_prov, prov) = get_providers();
    let prov = prov.with_max_events_level(5);

    event_prov.create(1, 1, CreateEventRequest {
        created_by: 0,
        tags: None,
        name: "e1".to_string(),
        display_name: None,
        description: None,
        status: events::Status::Enabled,
        is_system: false,
        properties: None,
        custom_properties: None,
    }).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "1".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event { id: 1, typ: EventType::Regular }],
    };
    prov.create(1, 1, req.clone()).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "2".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event { id: 1, typ: EventType::Custom }],
    };
    prov.create(1, 1, req.clone()).await?;

    let req = UpdateCustomEventRequest {
        updated_by: 0,
        tags: None,
        name: None,
        description: None,
        status: None,
        is_system: None,
        events: Some(vec![Event { id: 2, typ: EventType::Custom }]),
    };
    let resp = prov.update(1, 1, 1, req.clone()).await;
    assert!(matches!(resp,Err(MetadataError::CustomEvent(CustomEventError::DuplicateEvent))));

    // self-pointing
    let req = UpdateCustomEventRequest {
        updated_by: 0,
        tags: None,
        name: None,
        description: None,
        status: None,
        is_system: None,
        events: Some(vec![Event { id: 1, typ: EventType::Custom }]),
    };
    let resp = prov.update(1, 1, 1, req.clone()).await;
    assert!(matches!(resp,Err(MetadataError::CustomEvent(CustomEventError::DuplicateEvent))));
    Ok(())
}

#[tokio::test]
async fn update_event() -> Result<()> {
    let (event_prov, prov) = get_providers();

    let event = event_prov.create(1, 1, CreateEventRequest {
        created_by: 0,
        tags: None,
        name: "e1".to_string(),
        display_name: None,
        description: None,
        status: events::Status::Enabled,
        is_system: false,
        properties: None,
        custom_properties: None,
    }).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "some name".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event { id: event.id, typ: EventType::Regular }],
    };

    let resp = prov.create(1, 1, req.clone()).await?;
    assert_eq!(resp.name, req.name);

    let req = UpdateCustomEventRequest {
        updated_by: 0,
        tags: None,
        name: Some("name 2".to_string()),
        description: None,
        status: None,
        is_system: None,
        events: None,
    };

    let resp = prov.update(1, 1, 1, req).await?;
    assert_eq!(resp.name, "name 2".to_string());
    Ok(())
}

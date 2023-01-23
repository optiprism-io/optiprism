use std::env::temp_dir;
use std::sync::Arc;

use common::query::EventRef;
use common::types::OptionalProperty;
use metadata::custom_events::provider_impl::MAX_EVENTS_LEVEL;
use metadata::custom_events::CreateCustomEventRequest;
use metadata::custom_events::Event;
use metadata::custom_events::Provider;
use metadata::custom_events::ProviderImpl;
use metadata::custom_events::Status;
use metadata::custom_events::UpdateCustomEventRequest;
use metadata::error::CustomEventError;
use metadata::error::MetadataError;
use metadata::error::Result;
use metadata::events;
use metadata::events::CreateEventRequest;
use metadata::store::Store;
use uuid::Uuid;

fn get_providers(max_events_level: usize) -> (Arc<dyn events::Provider>, Arc<dyn Provider>) {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    let store = Arc::new(Store::new(path));
    let events_prov = Arc::new(events::ProviderImpl::new(store.clone()));
    let custom_events = Arc::new(
        ProviderImpl::new(store, events_prov.clone()).with_max_events_level(max_events_level),
    );

    (events_prov, custom_events)
}

#[tokio::test]
async fn non_exist() -> Result<()> {
    let (_, custom_events) = get_providers(MAX_EVENTS_LEVEL);

    // try to get, delete, update unexisting event
    assert!(custom_events.get_by_id(1, 1, 1).await.is_err());
    assert!(custom_events.delete(1, 1, 1).await.is_err());

    let update_event_req = UpdateCustomEventRequest {
        updated_by: 1,
        tags: OptionalProperty::None,
        name: OptionalProperty::None,
        description: OptionalProperty::None,
        status: OptionalProperty::None,
        is_system: OptionalProperty::None,
        events: OptionalProperty::None,
    };

    assert!(
        custom_events
            .update(1, 1, 1, update_event_req.clone())
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn create_event() -> Result<()> {
    let (event_prov, prov) = get_providers(MAX_EVENTS_LEVEL);

    let event = event_prov
        .create(1, 1, CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "e1".to_string(),
            display_name: None,
            description: None,
            status: events::Status::Enabled,
            is_system: false,
            properties: None,
            custom_properties: None,
        })
        .await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "some name".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event {
            event: EventRef::Regular(event.id),
            filters: None,
        }],
    };

    let resp = prov.create(1, 1, req.clone()).await?;
    assert_eq!(resp.name, req.name);
    let check = prov.get_by_id(1, 1, resp.id).await?;
    assert_eq!(resp, check);
    Ok(())
}

#[tokio::test]
async fn create_event_not_found() -> Result<()> {
    let (_event_prov, prov) = get_providers(MAX_EVENTS_LEVEL);

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "some name".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event {
            event: EventRef::Regular(1),
            filters: None,
        }],
    };

    let resp = prov.create(1, 1, req.clone()).await;
    assert!(resp.is_err());
    Ok(())
}

#[tokio::test]
async fn create_event_duplicate_name() -> Result<()> {
    let (event_prov, prov) = get_providers(MAX_EVENTS_LEVEL);

    event_prov
        .create(1, 1, CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "e1".to_string(),
            display_name: None,
            description: None,
            status: events::Status::Enabled,
            is_system: false,
            properties: None,
            custom_properties: None,
        })
        .await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "some name".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event {
            event: EventRef::Regular(1),
            filters: None,
        }],
    };

    let resp = prov.create(1, 1, req.clone()).await;
    assert!(resp.is_ok());
    let resp = prov.create(1, 1, req.clone()).await;
    assert!(resp.is_err());
    Ok(())
}

#[tokio::test]
async fn create_event_recursion_level_exceeded() -> Result<()> {
    let (event_prov, prov) = get_providers(1);

    event_prov
        .create(1, 1, CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "e1".to_string(),
            display_name: None,
            description: None,
            status: events::Status::Enabled,
            is_system: false,
            properties: None,
            custom_properties: None,
        })
        .await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "1".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event {
            event: EventRef::Regular(1),
            filters: None,
        }],
    };
    let _resp = prov.create(1, 1, req.clone()).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "2".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event {
            event: EventRef::Custom(1),
            filters: None,
        }],
    };
    let resp = prov.create(1, 1, req.clone()).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "3".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event {
            event: EventRef::Custom(resp.id),
            filters: None,
        }],
    };
    let resp = prov.create(1, 1, req.clone()).await;
    assert!(matches!(
        resp,
        Err(MetadataError::CustomEvent(
            CustomEventError::RecursionLevelExceeded(_)
        ))
    ));
    Ok(())
}

#[tokio::test]
async fn test_duplicate() -> Result<()> {
    let (event_prov, prov) = get_providers(5);

    event_prov
        .create(1, 1, CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "e1".to_string(),
            display_name: None,
            description: None,
            status: events::Status::Enabled,
            is_system: false,
            properties: None,
            custom_properties: None,
        })
        .await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "1".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event {
            event: EventRef::Regular(1),
            filters: None,
        }],
    };
    prov.create(1, 1, req.clone()).await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "2".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event {
            event: EventRef::Custom(1),
            filters: None,
        }],
    };
    prov.create(1, 1, req.clone()).await?;

    let req = UpdateCustomEventRequest {
        updated_by: 0,
        tags: OptionalProperty::None,
        name: OptionalProperty::None,
        description: OptionalProperty::None,
        status: OptionalProperty::None,
        is_system: OptionalProperty::None,
        events: OptionalProperty::Some(vec![Event {
            event: EventRef::Custom(2),
            filters: None,
        }]),
    };
    let resp = prov.update(1, 1, 1, req.clone()).await;
    assert!(matches!(
        resp,
        Err(MetadataError::CustomEvent(CustomEventError::DuplicateEvent))
    ));

    // self-pointing
    let req = UpdateCustomEventRequest {
        updated_by: 0,
        tags: OptionalProperty::None,
        name: OptionalProperty::None,
        description: OptionalProperty::None,
        status: OptionalProperty::None,
        is_system: OptionalProperty::None,
        events: OptionalProperty::Some(vec![Event {
            event: EventRef::Custom(1),
            filters: None,
        }]),
    };
    let resp = prov.update(1, 1, 1, req.clone()).await;
    assert!(matches!(
        resp,
        Err(MetadataError::CustomEvent(CustomEventError::DuplicateEvent))
    ));
    Ok(())
}

#[tokio::test]
async fn update_event() -> Result<()> {
    let (event_prov, prov) = get_providers(MAX_EVENTS_LEVEL);

    let _event = event_prov
        .create(1, 1, CreateEventRequest {
            created_by: 0,
            tags: None,
            name: "e1".to_string(),
            display_name: None,
            description: None,
            status: events::Status::Enabled,
            is_system: false,
            properties: None,
            custom_properties: None,
        })
        .await?;

    let req = CreateCustomEventRequest {
        created_by: 0,
        tags: None,
        name: "some name".to_string(),
        description: None,
        status: Status::Enabled,
        is_system: false,
        events: vec![Event {
            event: EventRef::Regular(1),
            filters: None,
        }],
    };

    let resp = prov.create(1, 1, req.clone()).await?;
    assert_eq!(resp.name, req.name);

    let req = UpdateCustomEventRequest {
        updated_by: 0,
        tags: OptionalProperty::None,
        name: OptionalProperty::Some("name 2".to_string()),
        description: OptionalProperty::None,
        status: OptionalProperty::None,
        is_system: OptionalProperty::None,
        events: OptionalProperty::None,
    };

    let resp = prov.update(1, 1, 1, req).await?;
    assert_eq!(resp.name, "name 2".to_string());
    Ok(())
}

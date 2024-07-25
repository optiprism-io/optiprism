use std::env::temp_dir;
use std::sync::Arc;

use common::query::EventRef;
use common::types::OptionalProperty;
use metadata::custom_events::CreateCustomEventRequest;
use metadata::custom_events::CustomEvents;
use metadata::custom_events::Event;
use metadata::custom_events::Status;
use metadata::custom_events::UpdateCustomEventRequest;
use metadata::custom_events::MAX_EVENTS_LEVEL;
use metadata::dictionaries::Dictionaries;
use metadata::error::MetadataError;
use metadata::error::Result;
use metadata::events;
use metadata::events::CreateEventRequest;
use metadata::events::Events;
use uuid::Uuid;

fn get_providers(max_events_level: usize) -> (Arc<Events>, Arc<CustomEvents>) {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));
    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let events_prov = Arc::new(events::Events::new(
        db.clone(),
        Arc::new(Dictionaries::new(db.clone())),
    ));
    let custom_events = Arc::new(
        CustomEvents::new(db, events_prov.clone()).with_max_events_level(max_events_level),
    );

    (events_prov, custom_events)
}

#[test]
fn non_exist() -> Result<()> {
    let (_, custom_events) = get_providers(MAX_EVENTS_LEVEL);

    // try to get, delete, update unexisting event
    assert!(custom_events.get_by_id(1, 1).is_err());
    assert!(custom_events.delete(1, 1).is_err());

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
            .update(1, 1, update_event_req.clone())
            .is_err()
    );
    Ok(())
}

#[test]
fn create_event() -> Result<()> {
    let (event_prov, prov) = get_providers(MAX_EVENTS_LEVEL);

    let event = event_prov.create(1, CreateEventRequest {
        created_by: 0,
        tags: None,
        name: "e1".to_string(),
        display_name: None,
        description: None,
        status: events::Status::Enabled,
        is_system: false,
        event_properties: None,
        user_properties: None,
        custom_properties: None,
    })?;

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

    let resp = prov.create(1, req.clone())?;
    let check = prov.get_by_id(1, resp.id)?;
    assert_eq!(resp.id, check.id);
    Ok(())
}

#[test]
fn create_event_not_found() -> Result<()> {
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

    let resp = prov.create(1, req.clone());
    assert!(resp.is_err());
    Ok(())
}

#[test]
fn create_event_duplicate_name() -> Result<()> {
    let (event_prov, prov) = get_providers(MAX_EVENTS_LEVEL);

    event_prov.create(1, CreateEventRequest {
        created_by: 0,
        tags: None,
        name: "e1".to_string(),
        display_name: None,
        description: None,
        status: events::Status::Enabled,
        is_system: false,
        event_properties: None,
        user_properties: None,
        custom_properties: None,
    })?;

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

    let resp = prov.create(1, req.clone());
    assert!(resp.is_ok());
    let resp = prov.create(1, req.clone());
    assert!(resp.is_err());
    Ok(())
}

#[test]
fn create_event_recursion_level_exceeded() -> Result<()> {
    let (event_prov, prov) = get_providers(1);

    event_prov.create(1, CreateEventRequest {
        created_by: 0,
        tags: None,
        name: "e1".to_string(),
        display_name: None,
        description: None,
        status: events::Status::Enabled,
        is_system: false,
        event_properties: None,
        user_properties: None,
        custom_properties: None,
    })?;

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
    let _resp = prov.create(1, req.clone())?;

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
    let resp = prov.create(1, req.clone())?;

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
    let resp = prov.create(1, req.clone());
    assert!(matches!(resp, Err(MetadataError::BadRequest(_))));
    Ok(())
}

#[test]
fn test_duplicate() -> Result<()> {
    let (event_prov, prov) = get_providers(5);

    event_prov.create(1, CreateEventRequest {
        created_by: 0,
        tags: None,
        name: "e1".to_string(),
        display_name: None,
        description: None,
        status: events::Status::Enabled,
        is_system: false,
        event_properties: None,
        custom_properties: None,
        user_properties: None,
    })?;

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
    prov.create(1, req.clone())?;

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
    prov.create(1, req.clone())?;

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
    let resp = prov.update(1, 1, req.clone());
    assert!(matches!(resp, Err(MetadataError::AlreadyExists(_))));

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
    let resp = prov.update(1, 1, req.clone());
    assert!(matches!(resp, Err(MetadataError::AlreadyExists(_))));
    Ok(())
}

#[test]
fn update_event() -> Result<()> {
    let (event_prov, prov) = get_providers(MAX_EVENTS_LEVEL);

    let _event = event_prov.create(1, CreateEventRequest {
        created_by: 0,
        tags: None,
        name: "e1".to_string(),
        display_name: None,
        description: None,
        status: events::Status::Enabled,
        is_system: false,
        event_properties: None,
        user_properties: None,
        custom_properties: None,
    })?;

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

    let resp = prov.create(1, req.clone())?;
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

    let resp = prov.update(1, 1, req)?;
    assert_eq!(resp.name, "name 2".to_string());
    Ok(())
}

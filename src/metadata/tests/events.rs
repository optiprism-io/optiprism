use std::env::temp_dir;
use std::sync::Arc;

use common::types::OptionalProperty;
use metadata::error::Result;
use metadata::events::CreateEventRequest;
use metadata::events::Events;
use metadata::events::Status;
use metadata::events::UpdateEventRequest;
use uuid::Uuid;
#[test]
fn test_events() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let events: Box<Events> = Box::new(Events::new(db.clone(), Arc::new(metadata::dictionaries::Dictionaries::new(db.clone()))));
    let create_event_req = CreateEventRequest {
        created_by: 0,
        tags: Some(vec![]),
        name: "".to_string(),
        display_name: None,
        description: None,
        status: Status::Enabled,
        event_properties: None,
        user_properties: None,
        custom_properties: None,
        is_system: false,
    };

    let update_event_req = UpdateEventRequest {
        updated_by: 1,
        tags: OptionalProperty::None,
        name: OptionalProperty::None,
        display_name: OptionalProperty::None,
        description: OptionalProperty::None,
        status: OptionalProperty::None,
        is_system: OptionalProperty::None,
        event_properties: Default::default(),
        custom_properties: OptionalProperty::None,
        user_properties: Default::default(),
    };

    // try to get, delete, update unexisting event
    assert!(events.get_by_id(1, 1).is_err());
    assert!(events.get_by_name(1, "test").is_err());
    assert!(events.delete(1, 1).is_err());
    assert!(events.update(1, 1, update_event_req.clone()).is_err());
    // assert_eq!(events.list_events()?, vec![]);
    // create two events

    let mut create_event1 = create_event_req.clone();
    create_event1.name = "event1".to_string();
    let res = events.get_or_create(1, create_event1.clone())?;
    assert_eq!(res.id, 1);
    let res = events.get_or_create(1, create_event1.clone())?;
    assert_eq!(res.id, 1);
    let mut create_event2 = create_event_req.clone();
    create_event2.name = "event2".to_string();
    let res = events.create(1, create_event2.clone())?;
    assert_eq!(res.id, 2);

    // events.attach_event_property(1, 1, 1)?;
    // assert!(events.attach_event_property(1, 1, 1).is_err());
    // events.attach_event_property(1, 1, 2)?;
    // assert!(events.detach_event_property(1, 1, 3).is_err());
    // events.detach_event_property(1, 1, 1)?;
    // events.attach_event_property(1, 1, 1)?;

    let mut create_event3 = create_event_req.clone();
    create_event3.name = "event3".to_string();
    let res = events.create(1, create_event3.clone())?;
    assert_eq!(res.id, 3);
    /*events.try_attach_properties(1, 3, vec![])?;
    events.try_attach_properties(1, 3, vec![1])?;
    events.try_attach_properties(1, 3, vec![1])?;
    events.try_attach_properties(1, 3, vec![1, 2])?;
    let e = events.get_by_id(1, 3)?;
    assert_eq!(e.event_properties, Some(vec![1, 2]));
    assert_eq!(e.user_properties, Some(vec![1, 2]));*/
    // check existence by id
    assert_eq!(events.get_by_id(1, 1)?.id, 1);
    assert_eq!(events.get_by_id(1, 2)?.id, 2);

    // by name
    assert_eq!(events.get_by_name(1, "event1")?.id, 1);
    assert_eq!(events.get_by_name(1, "event2")?.id, 2);
    let mut update_event1 = update_event_req.clone();
    update_event1.name.insert("event2".to_string());
    assert!(events.update(1, 1, update_event1.clone()).is_err());
    update_event1.name.insert("event1_new".to_string());
    update_event1.description.insert(Some("desc".to_string()));
    assert_eq!(events.update(1, 1, update_event1.clone())?.id, 1);

    assert!(events.get_by_name(1, "event1").is_err());
    let res = events.get_by_name(1, "event1_new")?;
    assert_eq!(res.id, 1);
    assert_eq!(
        OptionalProperty::Some(res.description),
        update_event1.description
    );

    update_event1.display_name.insert(Some("e".to_string()));
    assert_eq!(
        events.update(1, 1, update_event1.clone())?.display_name,
        Some("e".to_string())
    );

    let mut update_event2 = update_event_req.clone();
    update_event2.display_name.insert(Some("e".to_string()));
    assert!(events.update(1, 2, update_event2.clone()).is_err());
    update_event1.display_name.insert(Some("ee".to_string()));
    assert_eq!(
        events.update(1, 1, update_event1.clone())?.display_name,
        Some("ee".to_string())
    );
    assert_eq!(
        events.update(1, 2, update_event2.clone())?.display_name,
        Some("e".to_string())
    );

    assert_eq!(events.list(1)?.data[0].id, 1);

    // delete events
    assert_eq!(events.delete(1, 1)?.id, 1);
    assert_eq!(events.delete(1, 2)?.id, 2);

    // events should gone now
    assert!(events.get_by_id(1, 1).is_err());
    assert!(events.get_by_id(1, 2).is_err());
    assert!(events.get_by_name(1, "event1_new").is_err());
    Ok(())
}

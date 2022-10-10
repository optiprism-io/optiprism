use metadata::error::Result;
use metadata::store::Store;
use std::env::temp_dir;
use std::sync::Arc;

use common::types::OptionalProperty;
use metadata::events::types::CreateEventRequest;
use metadata::events::{Provider, Status, UpdateEventRequest};
use uuid::Uuid;

#[tokio::test]
async fn test_events() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let events = Provider::new(store.clone());
    let create_event_req = CreateEventRequest {
        created_by: 0,
        tags: Some(vec![]),
        name: "".to_string(),
        display_name: None,
        description: None,
        status: Status::Enabled,
        properties: None,
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
        properties: OptionalProperty::None,
        custom_properties: OptionalProperty::None,
    };

    // try to get, delete, update unexisting event
    assert!(events.get_by_id(1, 1, 1).await.is_err());
    assert!(events.get_by_name(1, 1, "test").await.is_err());
    assert!(events.delete(1, 1, 1).await.is_err());
    assert!(events
        .update(1, 1, 1, update_event_req.clone())
        .await
        .is_err());
    // assert_eq!(events.list_events().await?, vec![]);
    // create two events

    let mut create_event1 = create_event_req.clone();
    create_event1.name = "event1".to_string();
    let res = events.get_or_create(1, 1, create_event1.clone()).await?;
    assert_eq!(res.id, 1);
    let res = events.get_or_create(1, 1, create_event1.clone()).await?;
    assert_eq!(res.id, 1);
    let mut create_event2 = create_event_req.clone();
    create_event2.name = "event2".to_string();
    let res = events.create(1, 1, create_event2.clone()).await?;
    assert_eq!(res.id, 2);

    events.attach_property(1, 1, 1, 1).await?;
    assert!(events.attach_property(1, 1, 1, 1).await.is_err());
    events.attach_property(1, 1, 1, 2).await?;
    assert!(events.detach_property(1, 1, 1, 3).await.is_err());
    events.detach_property(1, 1, 1, 1).await?;
    events.attach_property(1, 1, 1, 1).await?;

    // check existence by id
    assert_eq!(events.get_by_id(1, 1, 1).await?.id, 1);
    assert_eq!(events.get_by_id(1, 1, 2).await?.id, 2);

    // by name
    assert_eq!(events.get_by_name(1, 1, "event1").await?.id, 1);
    assert_eq!(events.get_by_name(1, 1, "event2").await?.id, 2);
    let mut update_event1 = update_event_req.clone();
    update_event1.name.insert("event2".to_string());
    assert!(events.update(1, 1, 1, update_event1.clone()).await.is_err());
    update_event1.name.insert("event1_new".to_string());
    update_event1.description.insert(Some("desc".to_string()));
    assert_eq!(events.update(1, 1, 1, update_event1.clone()).await?.id, 1);

    assert!(events.get_by_name(1, 1, "event1").await.is_err());
    let res = events.get_by_name(1, 1, "event1_new").await?;
    assert_eq!(res.id, 1);
    assert_eq!(
        OptionalProperty::Some(res.description),
        update_event1.description
    );

    update_event1.display_name.insert(Some("e".to_string()));
    assert_eq!(
        events
            .update(1, 1, 1, update_event1.clone())
            .await?
            .display_name,
        Some("e".to_string())
    );

    let mut update_event2 = update_event_req.clone();
    update_event2.display_name.insert(Some("e".to_string()));
    assert!(events.update(1, 1, 2, update_event2.clone()).await.is_err());
    update_event1.display_name.insert(Some("ee".to_string()));
    assert_eq!(
        events
            .update(1, 1, 1, update_event1.clone())
            .await?
            .display_name,
        Some("ee".to_string())
    );
    assert_eq!(
        events
            .update(1, 1, 2, update_event2.clone())
            .await?
            .display_name,
        Some("e".to_string())
    );

    assert_eq!(events.list(1, 1).await?.data[0].id, 1);

    // delete events
    assert_eq!(events.delete(1, 1, 1).await?.id, 1);
    assert_eq!(events.delete(1, 1, 2).await?.id, 2);

    // events should gone now
    assert!(events.get_by_id(1, 1, 1).await.is_err());
    assert!(events.get_by_id(1, 1, 2).await.is_err());
    assert!(events.get_by_name(1, 1, "event1_new").await.is_err());
    Ok(())
}

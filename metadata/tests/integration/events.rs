use metadata::error::Result;
use metadata::store::store::Store;
use metadata::Metadata;
use std::env::temp_dir;
use std::sync::Arc;

use types::event::{CreateEventRequest, Event, Status, UpdateEventRequest};
use uuid::Uuid;

#[tokio::test]
async fn test_events() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let mut md = Metadata::try_new(store.clone())?;
    let create_event_req = CreateEventRequest {
        created_by: 0,
        updated_by: 0,
        project_id: 1,
        tags: vec![],
        name: "".to_string(),
        display_name: None,
        description: None,
        status: Status::Enabled,
        properties: None,
        custom_properties: None,
    };

    let update_event_req = UpdateEventRequest {
        id: 1,
        created_by: 0,
        update_by: 0,
        project_id: 1,
        tags: vec![],
        name: "".to_string(),
        display_name: None,
        description: None,
        status: Status::Enabled,
        properties: None,
        custom_properties: None,
    };

    // try to get, delete, update unexisting event
    assert!(md.events.get_event_by_id(1, 1).await.is_err());
    assert!(md.events.get_event_by_name(1, "test").await.is_err());
    assert!(md.events.delete_event(1, 1).await.is_err());
    assert!(md.events.update_event(update_event_req.clone()).await.is_err());
    // assert_eq!(md.events.list_events().await?, vec![]);
    // create two events
    let mut create_event1 = create_event_req.clone();
    create_event1.name = "event1".to_string();
    let res = md.events.create_event(create_event1.clone()).await?;
    assert_eq!(res.id, 1);
    let mut create_event2 = create_event_req.clone();
    create_event2.name = "event2".to_string();
    let res = md.events.create_event(create_event2.clone()).await?;
    assert_eq!(res.id, 2);
    // check existence by id
    assert_eq!(md.events.get_event_by_id(1, 1).await?.id, 1);
    assert_eq!(md.events.get_event_by_id(1, 2).await?.id, 2);

    // by name
    assert_eq!(md.events.get_event_by_name(1, "event1").await?.id, 1);
    assert_eq!(md.events.get_event_by_name(1, "event2").await?.id, 2);
    let mut update_event1 = update_event_req.clone();
    update_event1.id = 1;
    update_event1.name = "event1_new".to_string();
    assert_eq!(md.events.update_event(update_event1.clone()).await?.id, 1);

    assert!(md.events.get_event_by_name(1, "event1").await.is_err());
    assert_eq!(md.events.get_event_by_name(1, "event1_new").await?.id, 1);

    // TODO fix
    // assert_eq!(md.events.list_events().await?[0].id, 1);
    // assert_eq!(md.events.list_events().await?[1].id, 2);

    // delete events
    assert_eq!(md.events.delete_event(1, 1).await?.id, 1);
    assert_eq!(md.events.delete_event(1, 2).await?.id, 2);

    // events should gone now
    assert!(md.events.get_event_by_id(1, 1).await.is_err());
    assert!(md.events.get_event_by_id(1, 2).await.is_err());
    assert!(md.events.get_event_by_name(1, "event1_new").await.is_err());
    Ok(())
}

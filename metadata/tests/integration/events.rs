use metadata::error::Result;
use metadata::store::store::Store;
use metadata::Metadata;
use std::env::temp_dir;
use std::sync::Arc;

use uuid::Uuid;
use metadata::events::{Scope, Status, UpdateEventRequest};
use metadata::events::types::CreateEventRequest;

#[tokio::test]
async fn test_events() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let mut md = Metadata::try_new(store.clone())?;
    let create_event_req = CreateEventRequest {
        created_by: 0,
        project_id: 1,
        tags: vec![],
        name: "".to_string(),
        display_name: None,
        description: None,
        status: Status::Enabled,
        scope: Scope::System,
        properties: None,
        global_properties: None,
        custom_properties: None,
    };

    let update_event_req = UpdateEventRequest {
        id: 1,
        updated_by: 0,
        project_id: 1,
        tags: vec![],
        name: "".to_string(),
        display_name: None,
        description: None,
        status: Status::Enabled,
        scope: Scope::System,
        properties: None,
        global_properties: None,
        custom_properties: None,
    };

    // try to get, delete, update unexisting event
    assert!(md.events.get_by_id(1, 1, 1).await.is_err());
    assert!(md.events.get_by_name(1, 1, "test").await.is_err());
    assert!(md.events.delete(1, 1, 1).await.is_err());
    assert!(md.events.update(1, update_event_req.clone()).await.is_err());
    // assert_eq!(md.events.list_events().await?, vec![]);
    // create two events
    let mut create_event1 = create_event_req.clone();
    create_event1.name = "event1".to_string();
    let res = md.events.create(1, create_event1.clone()).await?;
    assert_eq!(res.id, 1);
    let mut create_event2 = create_event_req.clone();
    create_event2.name = "event2".to_string();
    let res = md.events.create(1, create_event2.clone()).await?;
    assert_eq!(res.id, 2);
    md.events.attach_property(1, 1, 1, 1).await?;
    assert!(md.events.attach_property(1, 1, 1, 1).await.is_err());
    md.events.attach_property(1, 1, 1, 2).await?;
    assert!(md.events.detach_property(1, 1, 1, 3).await.is_err());
    md.events.detach_property(1, 1, 1, 1).await?;
    md.events.attach_property(1, 1, 1, 1).await?;

    // check existence by id
    assert_eq!(md.events.get_by_id(1, 1, 1).await?.id, 1);
    assert_eq!(md.events.get_by_id(1, 1, 2).await?.id, 2);

    // by name
    assert_eq!(md.events.get_by_name(1, 1, "event1").await?.id, 1);
    assert_eq!(md.events.get_by_name(1, 1, "event2").await?.id, 2);
    let mut update_event1 = update_event_req.clone();
    update_event1.id = 1;
    update_event1.name = "event2".to_string();
    assert!(md.events.update(1, update_event1.clone()).await.is_err());
    update_event1.name = "event1_new".to_string();
    update_event1.description = Some("desc".to_string());
    assert_eq!(md.events.update(1, update_event1.clone()).await?.id, 1);

    assert!(md.events.get_by_name(1, 1, "event1").await.is_err());
    let res = md.events.get_by_name(1, 1, "event1_new").await?;
    assert_eq!(res.id, 1);
    assert_eq!(res.description, update_event1.description);

    update_event1.display_name = Some("e".to_string());
    assert_eq!(md.events.update(1, update_event1.clone()).await?.display_name, Some("e".to_string()));

    let mut update_event2 = update_event_req.clone();
    update_event2.id = 2;
    update_event2.display_name = Some("e".to_string());
    assert!(md.events.update(1, update_event2.clone()).await.is_err());
    update_event1.display_name = Some("ee".to_string());
    assert_eq!(md.events.update(1, update_event1.clone()).await?.display_name, Some("ee".to_string()));
    assert_eq!(md.events.update(1, update_event2.clone()).await?.display_name, Some("e".to_string()));
    // TODO fix
    // assert_eq!(md.events.list_events().await?[0].id, 1);
    // assert_eq!(md.events.list_events().await?[1].id, 2);

    // delete events
    assert_eq!(md.events.delete(1, 1, 1).await?.id, 1);
    assert_eq!(md.events.delete(1, 1, 2).await?.id, 2);

    // events should gone now
    assert!(md.events.get_by_id(1, 1, 1).await.is_err());
    assert!(md.events.get_by_id(1, 1, 2).await.is_err());
    assert!(md.events.get_by_name(1, 1, "event1_new").await.is_err());
    Ok(())
}

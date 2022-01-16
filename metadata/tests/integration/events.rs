use metadata::error::Result;
use metadata::store::store::Store;
use metadata::Metadata;
use std::env::temp_dir;
use std::sync::Arc;

use types::event::{Event, Status};
use uuid::Uuid;

#[tokio::test]
async fn test_events() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let mut md = Metadata::try_new(store.clone())?;
    let event_tpl = Event {
        id: 0,
        created_at: None,
        updated_at: None,
        created_by: 0,
        update_by: 0,
        project_id: 0,
        is_system: false,
        tags: vec![],
        name: "".to_string(),
        display_name: None,
        description: None,
        status: Status::Enabled,
        properties: None,
        custom_properties: None,
    };
    // try to get, delete, update unexisting event
    assert!(md.events.get_event_by_id(1).await.is_err());
    assert!(md.events.get_event_by_name("test").await.is_err());
    assert!(md.events.delete_event(1).await.is_err());
    assert!(md.events.update_event(event_tpl.clone()).await.is_err());
    assert_eq!(md.events.list_events().await?, vec![]);

    // create two events
    let mut event1 = event_tpl.clone();
    event1.id = 1;
    event1.name = "event1".to_string();
    let res = md.events.create_event(event1.clone()).await?;
    assert_eq!(res.id, 1);

    let mut event2 = event_tpl.clone();
    event2.id = 2;
    event2.name = "event2".to_string();
    let res = md.events.create_event(event2.clone()).await?;
    assert_eq!(res.id, 2);

    // check existence by id
    assert_eq!(md.events.get_event_by_id(1).await?.id, 1);
    assert_eq!(md.events.get_event_by_id(2).await?.id, 2);

    // by name
    assert_eq!(md.events.get_event_by_name("event1").await?.id, 1);
    assert_eq!(md.events.get_event_by_name("event2").await?.id, 2);

    event1.name = "event1_new".to_string();
    assert_eq!(md.events.update_event(event1.clone()).await?.id, 1);

    assert!(md.events.get_event_by_name("event1").await.is_err());
    assert_eq!(md.events.get_event_by_name("event1_new").await?.id, 1);

    assert_eq!(md.events.list_events().await?[0].id, 1);
    assert_eq!(md.events.list_events().await?[1].id, 2);

    // delete events
    assert_eq!(md.events.delete_event(1).await?.id, 1);
    assert_eq!(md.events.delete_event(2).await?.id, 2);

    // events should gone now
    assert!(md.events.get_event_by_id(1).await.is_err());
    assert!(md.events.get_event_by_id(2).await.is_err());
    assert!(md.events.get_event_by_name("event1_new").await.is_err());
    Ok(())
}

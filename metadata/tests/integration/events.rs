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
    let md = Metadata::new(store.clone());

    let event = Event {
        id: 1,
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
    assert_eq!(md.events.get_event(1).await?, None);
    assert_eq!(md.events.delete_event(1).await?, None);
    assert_eq!(md.events.update_event(event.clone()).await?, None);
    assert_eq!(md.events.list_events().await?, vec![]);

    // create two events
    let res = md.events.create_event(event.clone()).await?;
    assert_eq!(res.id, 1);
    let res = md.events.create_event(event.clone()).await?;
    assert_eq!(res.id, 2);

    // check existence
    assert_eq!(md.events.get_event(1).await?.unwrap().id, 1);
    assert_eq!(md.events.get_event(2).await?.unwrap().id, 2);
    assert_eq!(md.events.update_event(event.clone()).await?.unwrap().id, 1);
    assert_eq!(md.events.list_events().await?[0].id, 1);
    assert_eq!(md.events.list_events().await?[1].id, 2);

    // delete events
    assert_eq!(md.events.delete_event(1).await?.unwrap().id, 1);
    assert_eq!(md.events.delete_event(2).await?.unwrap().id, 2);

    // events should gone now
    assert_eq!(md.events.get_event(1).await?, None);
    assert_eq!(md.events.get_event(2).await?, None);
    Ok(())
}

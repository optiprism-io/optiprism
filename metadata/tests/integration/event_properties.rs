use metadata::error::Result;
use metadata::store::store::Store;
use metadata::Metadata;
use std::env::temp_dir;
use std::sync::Arc;
use arrow::datatypes::DataType;
use metadata::event_properties::{CreateEventPropertyRequest, UpdateEventPropertyRequest, Status, Scope};
use uuid::Uuid;

#[tokio::test]
async fn test_properties() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let mut md = Metadata::try_new(store.clone())?;
    let create_prop_req = CreateEventPropertyRequest {
        created_by: 0,
        project_id: 1,
        scope: Scope::User,
        tags: vec![],
        name: "prop1".to_string(),
        description: "".to_string(),
        display_name: None,
        typ: DataType::Null,
        status: Status::Enabled,
        nullable: false,
        is_array: false,
        is_dictionary: false,
        dictionary_type: None,
    };

    let update_prop_req = UpdateEventPropertyRequest {
        id: 1,
        created_by: 0,
        updated_by: None,
        project_id: 1,
        scope: Scope::User,
        tags: vec![],
        name: "".to_string(),
        description: "".to_string(),
        display_name: None,
        typ: DataType::Null,
        status: Status::Enabled,
        nullable: false,
        is_array: false,
        is_dictionary: false,
        dictionary_type: None,
    };


    // try to get, delete, update unexisting event prop
    assert!(md.event_properties.get_by_id(1, 1, 1).await.is_err());
    assert!(md.event_properties.get_by_name(1, 1, "test").await.is_err());
    assert!(md.event_properties.delete(1, 1, 1).await.is_err());
    assert!(md.event_properties.update(1, update_prop_req.clone()).await.is_err());
    let mut create_prop1 = create_prop_req.clone();
    create_prop1.name = "prop1".to_string();
    let res = md.event_properties.create(1, create_prop1.clone()).await?.id;
    assert_eq!(res, 1);
    let mut create_prop2 = create_prop_req.clone();
    create_prop2.name = "prop2".to_string();
    let res = md.event_properties.create(1, create_prop2.clone()).await?.id;
    assert_eq!(res, 2);
    // check existence by id
    assert_eq!(md.event_properties.get_by_id(1, 1, 1).await?.id, 1);
    assert_eq!(md.event_properties.get_by_id(1, 1, 2).await?.id, 2);
    // by name
    assert_eq!(md.event_properties.get_by_name(1, 1, "prop1").await?.id, 1);
    assert_eq!(md.event_properties.get_by_name(1, 1, "prop2").await?.id, 2);
    let mut update_event1 = update_prop_req.clone();
    update_event1.id = 1;
    update_event1.name = "prop2".to_string();
    assert!(md.event_properties.update(1, update_event1.clone()).await.is_err());
    update_event1.name = "prop1_new".to_string();
    update_event1.scope = Scope::User;
    let res = md.event_properties.update(1, update_event1.clone()).await?;
    assert_eq!(res.id, 1);
    assert_eq!(res.scope, Scope::User);

    assert!(md.event_properties.get_by_name(1, 1, "prop1").await.is_err());
    assert_eq!(md.event_properties.get_by_name(1, 1, "prop1_new").await?.id, 1);

    update_event1.display_name = Some("e".to_string());
    assert_eq!(md.event_properties.update(1, update_event1.clone()).await?.display_name, Some("e".to_string()));

    let mut update_event2 = update_prop_req.clone();
    update_event2.id = 2;
    update_event2.display_name = Some("e".to_string());
    assert!(md.event_properties.update(1, update_event2.clone()).await.is_err());
    update_event1.display_name = Some("ee".to_string());
    assert_eq!(md.event_properties.update(1, update_event1.clone()).await?.display_name, Some("ee".to_string()));
    assert_eq!(md.event_properties.update(1, update_event2.clone()).await?.display_name, Some("e".to_string()));
    // TODO fix
    // assert_eq!(md.event_properties.list().await?[0].id, 1);
    // assert_eq!(md.event_properties.list().await?[1].id, 2);


    // delete props
    assert_eq!(md.event_properties.delete(1, 1, 1).await?.id, 1);
    assert!(md.event_properties.get_by_id(1, 1, 1).await.is_err());
    assert!(md.event_properties.get_by_name(1, 1, "prop1_new").await.is_err());
    assert_eq!(md.event_properties.delete(1, 1, 2).await?.id, 2);
    assert!(md.event_properties.get_by_id(1, 1, 2).await.is_err());

    Ok(())
}

use arrow::datatypes::DataType;
use common::types::OptionalProperty;
use metadata::error::Result;
use metadata::properties::provider::Namespace;
use metadata::properties::{CreatePropertyRequest, Provider, Status, UpdatePropertyRequest};
use metadata::store::Store;
use std::env::temp_dir;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_properties() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let event_properties = Provider::new_event(store.clone());
    let create_prop_req = CreatePropertyRequest {
        created_by: 0,
        tags: Some(vec![]),
        name: "prop1".to_string(),
        description: Some("".to_string()),
        display_name: None,
        typ: DataType::Null,
        status: Status::Enabled,
        is_system: false,
        nullable: false,
        is_array: false,
        is_dictionary: false,
        dictionary_type: None,
    };

    let update_prop_req = UpdatePropertyRequest {
        updated_by: 1,
        tags: OptionalProperty::None,
        name: OptionalProperty::None,
        description: OptionalProperty::None,
        display_name: OptionalProperty::None,
        typ: OptionalProperty::None,
        status: OptionalProperty::None,
        is_system: OptionalProperty::None,
        nullable: OptionalProperty::None,
        is_array: OptionalProperty::None,
        is_dictionary: OptionalProperty::None,
        dictionary_type: OptionalProperty::None,
    };

    // try to get, delete, update unexisting event prop
    assert!(event_properties.get_by_id(1, 1, 1).await.is_err());
    assert!(event_properties.get_by_name(1, 1, "test").await.is_err());
    assert!(event_properties.delete(1, 1, 1).await.is_err());
    assert!(event_properties
        .update(1, 1, 1, update_prop_req.clone())
        .await
        .is_err());
    let mut create_prop1 = create_prop_req.clone();
    create_prop1.name = "prop1".to_string();
    let res = event_properties
        .get_or_create(1, 1, create_prop1.clone())
        .await?
        .id;
    assert_eq!(res, 1);
    let res = event_properties
        .get_or_create(1, 1, create_prop1.clone())
        .await?;
    assert_eq!(res.id, 1);

    assert_eq!(res.column_name(Namespace::User), "user_prop_1".to_string());

    let mut create_prop2 = create_prop_req.clone();
    create_prop2.name = "prop2".to_string();
    let res = event_properties
        .create(1, 1, create_prop2.clone())
        .await?
        .id;
    assert_eq!(res, 2);
    // check existence by id
    assert_eq!(event_properties.get_by_id(1, 1, 1).await?.id, 1);
    assert_eq!(event_properties.get_by_id(1, 1, 2).await?.id, 2);
    // by name
    assert_eq!(event_properties.get_by_name(1, 1, "prop1").await?.id, 1);
    assert_eq!(event_properties.get_by_name(1, 1, "prop2").await?.id, 2);
    let mut update_prop1 = update_prop_req.clone();
    update_prop1.name.insert("prop2".to_string());
    assert!(event_properties
        .update(1, 1, 1, update_prop1.clone())
        .await
        .is_err());
    update_prop1.name.insert("prop1_new".to_string());
    let res = event_properties
        .update(1, 1, 1, update_prop1.clone())
        .await?;
    assert_eq!(res.id, 1);

    assert!(event_properties.get_by_name(1, 1, "prop1").await.is_err());
    assert_eq!(event_properties.get_by_name(1, 1, "prop1_new").await?.id, 1);

    update_prop1.display_name.insert(Some("e".to_string()));
    assert_eq!(
        event_properties
            .update(1, 1, 1, update_prop1.clone())
            .await?
            .display_name,
        Some("e".to_string())
    );

    let mut update_prop2 = update_prop_req.clone();
    update_prop2.display_name.insert(Some("e".to_string()));
    assert!(event_properties
        .update(1, 1, 2, update_prop2.clone())
        .await
        .is_err());
    update_prop1.display_name.insert(Some("ee".to_string()));
    assert_eq!(
        event_properties
            .update(1, 1, 1, update_prop1.clone())
            .await?
            .display_name,
        Some("ee".to_string())
    );
    assert_eq!(
        event_properties
            .update(1, 1, 2, update_prop2.clone())
            .await?
            .display_name,
        Some("e".to_string())
    );

    assert_eq!(event_properties.list(1, 1).await?.data[0].id, 1);

    // delete props
    assert_eq!(event_properties.delete(1, 1, 1).await?.id, 1);
    assert!(event_properties.get_by_id(1, 1, 1).await.is_err());
    assert!(event_properties
        .get_by_name(1, 1, "prop1_new")
        .await
        .is_err());
    assert_eq!(event_properties.delete(1, 1, 2).await?.id, 2);
    assert!(event_properties.get_by_id(1, 1, 2).await.is_err());

    Ok(())
}

use std::env::temp_dir;
use std::sync::Arc;
use rocksdb::TransactionDB;

use common::types::{DType, OptionalProperty};
use metadata::error::Result;
use metadata::properties::CreatePropertyRequest;
use metadata::properties::Provider;
use metadata::properties::ProviderImpl;
use metadata::properties::Status;
use metadata::properties::Type;
use metadata::properties::UpdatePropertyRequest;
use uuid::Uuid;
use metadata::test_util::init_db;
use store::db::{OptiDBImpl, Options, TableOptions};

#[test]
fn test_properties() -> Result<()> {
    let (md, opti_db) = init_db()?;

    // try to get, delete, update unexisting event prop
    assert!(md.event_properties.get_by_id(1, 1, 1).is_err());
    assert!(md.event_properties.get_by_name(1, 1, "test").is_err());
    assert!(md.event_properties.delete(1, 1, 1).is_err());

    let update_prop_req = UpdatePropertyRequest {
        updated_by: 1,
        tags: OptionalProperty::None,
        name: OptionalProperty::None,
        description: OptionalProperty::None,
        display_name: OptionalProperty::None,
        typ: OptionalProperty::None,
        data_type: OptionalProperty::None,
        status: OptionalProperty::None,
        is_system: OptionalProperty::None,
        nullable: OptionalProperty::None,
        is_array: OptionalProperty::None,
        is_dictionary: OptionalProperty::None,
        dictionary_type: OptionalProperty::None,
    };

    assert!(
        md.event_properties
            .update(1, 1, 1, update_prop_req.clone())
            .is_err()
    );

    let create_prop_req = CreatePropertyRequest {
        created_by: 0,
        tags: Some(vec![]),
        name: "prop1".to_string(),
        description: Some("".to_string()),
        display_name: None,
        typ: Type::Event,
        data_type: DType::String,
        status: Status::Enabled,
        is_system: false,
        nullable: false,
        is_array: false,
        is_dictionary: false,
        dictionary_type: None,
    };

    let res = md.event_properties
        .get_or_create(1, 1, create_prop_req.clone())?
        .id;

    assert_eq!(res, 1);
    let res = md.event_properties.get_or_create(1, 1, create_prop_req.clone())?;
    assert_eq!(res.id, 1);

    assert_eq!(res.column_name(), "str_0".to_string());
    assert_eq!(res.order, 0);

    let mut create_prop2 = create_prop_req.clone();
    create_prop2.name = "prop2".to_string();
    let res = md.event_properties.create(1, 1, create_prop2.clone())?;
    assert_eq!(res.id, 2);
    assert_eq!(res.column_name(), "str_1".to_string());
    assert_eq!(res.order, 1);
    let mut create_prop3 = create_prop_req.clone();
    create_prop3.name = "prop3".to_string();
    create_prop3.data_type = DType::Int64;
    let res = md.event_properties.create(1, 1, create_prop3.clone())?;
    assert_eq!(res.id, 3);
    assert_eq!(res.order, 0);
    // check existence by id
    assert_eq!(md.event_properties.get_by_id(1, 1, 1)?.id, 1);
    assert_eq!(md.event_properties.get_by_id(1, 1, 2)?.id, 2);
    // by name

    assert_eq!(md.event_properties.get_by_name(1, 1, "prop1")?.id, 1);
    assert_eq!(md.event_properties.get_by_name(1, 1, "prop2")?.id, 2);
    let mut update_prop1 = update_prop_req.clone();
    update_prop1.name.insert("prop2".to_string());
    let a = md.event_properties
        .update(1, 1, 1, update_prop1.clone());
    assert_eq!(
        md.event_properties
            .update(1, 1, 1, update_prop1.clone())?.name, "prop1".to_string());
    update_prop1.name.insert("prop1_new".to_string());
    let res = md.event_properties.update(1, 1, 1, update_prop1.clone())?;
    assert_eq!(res.id, 1);

    assert!(md.event_properties.get_by_name(1, 1, "prop1").is_ok());

    update_prop1.display_name.insert(Some("e".to_string()));
    assert_eq!(
        md.event_properties
            .update(1, 1, 1, update_prop1.clone())?
            .display_name,
        Some("e".to_string())
    );

    let mut update_prop2 = update_prop_req.clone();
    update_prop2.display_name.insert(Some("e".to_string()));
    assert!(
        md.event_properties
            .update(1, 1, 2, update_prop2.clone())
            .is_err()
    );
    update_prop1.display_name.insert(Some("ee".to_string()));
    assert_eq!(
        md.event_properties
            .update(1, 1, 1, update_prop1.clone())?
            .display_name,
        Some("ee".to_string())
    );
    assert_eq!(
        md.event_properties
            .update(1, 1, 2, update_prop2.clone())?
            .display_name,
        Some("e".to_string())
    );

    assert_eq!(md.event_properties.list(1, 1)?.data[0].id, 1);

    // delete props
    assert_eq!(md.event_properties.delete(1, 1, 1)?.id, 1);
    assert!(md.event_properties.get_by_id(1, 1, 1).is_err());
    assert!(md.event_properties.get_by_name(1, 1, "prop1_new").is_err());
    assert_eq!(md.event_properties.delete(1, 1, 2)?.id, 2);
    assert!(md.event_properties.get_by_id(1, 1, 2).is_err());

    Ok(())
}

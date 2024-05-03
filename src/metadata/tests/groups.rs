use std::env::temp_dir;
use std::sync::Arc;

use common::types::OptionalProperty;
use metadata::error::Result;
use metadata::events::CreateEventRequest;
use metadata::events::Events;
use metadata::events::Status;
use metadata::events::UpdateEventRequest;
use metadata::groups::Groups;
use metadata::groups::PropertyValue;
use metadata::groups::Value;
use uuid::Uuid;
#[test]
fn test_groups() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let groups = Groups::new(db.clone());

    let pv1 = PropertyValue {
        property_id: 1,
        value: Value::Int64(Some(1)),
    };
    let group = groups.get_or_create(1, 1, "u1", vec![pv1.clone()]).unwrap();
    assert_eq!(group.id, 1);
    let group = groups.get_or_create(1, 1, "u2", vec![pv1]).unwrap();
    assert_eq!(group.id, 2);

    let group = groups
        .merge_with_anonymous(1, 1, "a1", "u1", vec![])
        .unwrap();
    assert_eq!(group.id, 1);

    let group = groups
        .merge_with_anonymous(1, 1, "a1", "u2", vec![])
        .unwrap();
    assert_eq!(group.id, 2);

    let group = groups
        .merge_with_anonymous(1, 1, "a1", "u3", vec![])
        .unwrap();
    assert_eq!(group.id, 3);

    let group = groups
        .merge_with_anonymous(1, 1, "a2", "u3", vec![])
        .unwrap();
    assert_eq!(group.id, 3);
    Ok(())
}

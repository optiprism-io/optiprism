use std::env::temp_dir;
use std::sync::Arc;

use metadata::dictionaries::Dictionaries;
use metadata::dictionaries::Provider;
use metadata::error::Result;
use uuid::Uuid;
#[test]
fn test_dictionaries() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let dicts: Box<dyn Provider> = Box::new(Dictionaries::new(db.clone()));

    assert!(dicts.get_key(1, 1, "d1", "v1").is_err());
    assert!(dicts.get_value(1, 1, "d1", 1).is_err());

    assert_eq!(dicts.get_key_or_create(1, 1, "d1", "d1v1")?, 1);
    assert_eq!(dicts.get_key(1, 1, "d1", "d1v1")?, 1);
    assert_eq!(dicts.get_value(1, 1, "d1", 1)?, "d1v1");
    assert_eq!(dicts.get_key_or_create(1, 1, "d1", "d1v1")?, 1);

    assert_eq!(dicts.get_key_or_create(1, 1, "d1", "d1v2")?, 2);
    assert_eq!(dicts.get_key_or_create(1, 1, "d2", "d2v1")?, 1);
    assert_eq!(dicts.get_key_or_create(1, 1, "d2", "d2v2")?, 2);
    assert_eq!(dicts.get_key(1, 1, "d2", "d2v1")?, 1);
    assert_eq!(dicts.get_value(1, 1, "d2", 1)?, "d2v1");
    Ok(())
}

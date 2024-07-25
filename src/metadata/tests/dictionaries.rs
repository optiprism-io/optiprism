use std::env::temp_dir;
use std::sync::Arc;

use metadata::dictionaries::Dictionaries;
use metadata::error::Result;
use uuid::Uuid;
#[test]
fn test_dictionaries() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let dicts: Box<Dictionaries> = Box::new(Dictionaries::new(db.clone()));

    assert!(dicts.get_key(1, "t1","d1", "v1").is_err());
    assert!(dicts.get_value(1, "t1","d1", 1).is_err());

    assert_eq!(dicts.get_key_or_create(1, "t1","d1",  "v1")?, 1);
    assert_eq!(dicts.get_key(1, "t1", "d1", "v1")?, 1);
    assert_eq!(dicts.get_value(1, "t1","d1", 1)?, "v1");
    assert_eq!(dicts.get_key_or_create(1, "t1","d1",  "v1")?, 1);

    assert_eq!(dicts.get_key_or_create(1, "t1","d1",  "v2")?, 2);
    assert_eq!(dicts.get_key_or_create(1, "t1","d2",  "v1")?, 1);
    assert_eq!(dicts.get_key_or_create(1, "t1","d2", "v2")?, 2);
    assert_eq!(dicts.get_key(1, "t1","d2",  "v1")?, 1);
    assert_eq!(dicts.get_value(1, "t1","d2", 1)?,"v1");
    Ok(())
}

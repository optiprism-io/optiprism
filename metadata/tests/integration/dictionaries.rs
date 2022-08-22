use metadata::dictionaries::Provider;
use metadata::error::Result;
use std::env::temp_dir;
use std::sync::Arc;
use uuid::Uuid;
use metadata::store::Store;

#[tokio::test]
async fn test_dictionaries() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let store = Arc::new(Store::new(path));
    let dicts = Provider::new(store.clone());

    assert!(dicts.get_key(1, 1, "d1", "v1").await.is_err());
    assert!(dicts.get_value(1, 1, "d1", 1).await.is_err());

    assert_eq!(dicts.get_key_or_create(1, 1, "d1", "d1v1").await?, 1);
    assert_eq!(dicts.get_key(1, 1, "d1", "d1v1").await?, 1);
    assert_eq!(dicts.get_value(1, 1, "d1", 1).await?, "d1v1");
    assert_eq!(dicts.get_key_or_create(1, 1, "d1", "d1v1").await?, 1);

    assert_eq!(dicts.get_key_or_create(1, 1, "d1", "d1v2").await?, 2);
    assert_eq!(dicts.get_key_or_create(1, 1, "d2", "d2v1").await?, 1);
    assert_eq!(dicts.get_key_or_create(1, 1, "d2", "d2v2").await?, 2);
    assert_eq!(dicts.get_key(1, 1, "d2", "d2v1").await?, 1);
    assert_eq!(dicts.get_value(1, 1, "d2", 1).await?, "d2v1");
    Ok(())
}

use std::env::temp_dir;
use std::sync::Arc;

use metadata::error::Result;
use uuid::Uuid;
use metadata::settings::{SettingsProvider, StringKey};

#[test]
fn test_config() -> Result<()> {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let cfg: Box<SettingsProvider> = Box::new(SettingsProvider::new(db.clone()));

    assert!(cfg.get_string(StringKey::AuthAccessToken).is_err());
    assert!(cfg.set_string(StringKey::AuthAccessToken, Some("test".to_string())).is_ok());
    let v = cfg.get_string(StringKey::AuthAccessToken).unwrap();

    assert_eq!(v, Some("test".to_string()));

    assert!(cfg.set_string(StringKey::AuthAccessToken, None).is_ok());
    let v = cfg.get_string(StringKey::AuthAccessToken).unwrap();

    assert_eq!(v, None);
    Ok(())
}

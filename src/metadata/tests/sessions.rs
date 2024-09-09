use std::env::temp_dir;
use std::sync::{Arc, Mutex};
use chrono::DateTime;
use uuid::Uuid;
use metadata::sessions::Sessions;

#[test]
fn test_sessions() {
    let mut path = temp_dir();
    path.push(format!("{}.db", Uuid::new_v4()));

    let db = Arc::new(metadata::rocksdb::new(path).unwrap());
    let sessions = Box::new(Sessions::new(db.clone()));

    sessions.check_for_deletion(1, |_s| {
        Ok(true)
    }).unwrap();

    let f = sessions.set_current_time(1, 1, DateTime::from_timestamp(1, 0).unwrap()).unwrap();
    assert!(f);
    let f = sessions.set_current_time(1, 2, DateTime::from_timestamp(1, 0).unwrap()).unwrap();
    assert!(f);
    let f = sessions.set_current_time(1, 1, DateTime::from_timestamp(1, 0).unwrap()).unwrap();
    assert!(!f);

    sessions.clear_project(1).unwrap();

    let f = sessions.set_current_time(1, 1, DateTime::from_timestamp(1, 0).unwrap()).unwrap();
    assert!(f);

    let found = Arc::new(Mutex::new(false));
    let found2 = found.clone();
    sessions.check_for_deletion(1, |_s| {
        let mut f = found2.lock().unwrap();
        *f = true;
        Ok(true)
    }).unwrap();
    assert!(*found.lock().unwrap());
    let f = sessions.set_current_time(1, 1, DateTime::from_timestamp(1, 0).unwrap()).unwrap();
    assert!(f);
}

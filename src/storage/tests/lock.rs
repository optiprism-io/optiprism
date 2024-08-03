use std::sync::{Arc, RwLock};
use std::thread;

#[test]
fn test_lock() {
    let mx = Arc::new(RwLock::new(()));
    let mx_clone = mx.clone();
    thread::spawn(move || {
        let _g = mx_clone.read().unwrap();
        println!("read");
        thread::sleep(std::time::Duration::from_secs(2));
        println!("closed");
    });
    thread::sleep(std::time::Duration::from_secs(1));
    let _sd = mx.write().unwrap();
    println!("write");
    drop(_sd);
}
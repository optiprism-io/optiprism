use std::thread;
use std::time::Duration;

fn main() {
    for s in 0..30 {
        let v = s;
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));
                println!("Hello, world! {v}");
            }
        });
    }

    thread::sleep(Duration::from_secs(1000));
}

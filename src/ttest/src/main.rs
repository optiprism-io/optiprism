#![feature(async_closure)]

use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::thread;
use std::thread::spawn;
use std::time::Duration;

use futures::executor::block_on;
use futures::prelude::*;

async fn sender(i: i32) {
    println!("!");
}

fn main() {
    let mut handles = vec![];
    let (tx, rx) = channel::<i32>();
    for i in 0..5 {
        let handle = block_on(tokio::spawn(async move { sender(i).await }));
        handles.push(handle);
    }
    loop {}
}

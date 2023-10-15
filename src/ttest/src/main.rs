#![feature(async_closure)]

use std::sync::mpsc::channel;

use futures::executor::block_on;

async fn sender(_i: i32) {
    println!("!");
}

fn main() {}

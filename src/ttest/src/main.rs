#![feature(async_closure)]
extern crate maxminddb;

use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;

use futures::executor::block_on;

// creates a number of threads and waits for them to finish
fn main() {
    let a: Vec<dyn PartialOrd<i32>> = vec![1, 3, 3];
    let b: Vec<dyn PartialOrd<i32>> = vec![1, 2, 3];
    println!("{:?}", a.partial_cmp(&b));
}

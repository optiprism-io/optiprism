#![feature(async_closure)]

use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;

use futures::executor::block_on;

fn spawn_processing_thread() -> mpsc::Sender<(Request, oneshot::Sender<usize>)> {
    let (request_sender, request_receiver) = mpsc::channel::<(Request, oneshot::Sender<usize>)>();
    thread::spawn(move || {
        for (request_data, response_sender) in request_receiver.iter() {
            let compute_operation = || request_data.len();
            let _ = response_sender.send(compute_operation()); // <- Send on the oneshot channel
        }
    });
    request_sender
}
// creates a number of threads and waits for them to finish
fn main() {
    let (tx, rx) = channel();

    for i in 0..10 {
        let tx = tx.clone();
        std::thread::spawn(move || run(tx));
    }

    for i in rx {
        println!("{}", i);
    }
}

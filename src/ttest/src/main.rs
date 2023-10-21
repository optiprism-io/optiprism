#![feature(async_closure)]
extern crate maxminddb;

use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;

use futures::executor::block_on;
// creates a number of threads and waits for them to finish
fn main() {
    use std::net::IpAddr;
    use std::str::FromStr;

    use maxminddb::geoip2;

    let reader = maxminddb::Reader::open_readfile(
        "/Users/maximbogdanov/optiprism/optiprism/data/GeoLite2-City_20231020/GeoLite2-City.mmdb",
    )
    .unwrap();

    let ip: IpAddr = FromStr::from_str("89.160.20.128").unwrap();
    let city: geoip2::City = reader.lookup(ip).unwrap();
    print!("{:?}", city);
}

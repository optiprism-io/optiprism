use crate::exprtree::error::{Error, Result};
use crate::ifaces::dictionary::Record;
use std::net::IpAddr;

struct Geo {
    country: Option<Record>,
    region: Option<Record>,
    city: Option<Record>,
    isp: Option<Record>,
    connection_type: Option<Record>,
}

trait GeoProvider {
    fn lookup(&self, ip: IpAddr) -> Result<Geo>;
}

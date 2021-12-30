use crate::exprtree::error::{Error, Result};
use crate::ifaces::dictionary::Record;
use std::net::IpAddr;

struct Device {
    browser_name: Option<Record>,
    browser_version: Option<Record>,
}

trait DeviceProvider {
    fn parse_user_agent(&self, user_agent: String) -> Result<Device>;
}

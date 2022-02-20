use crate::Result;
use geoip2::{City, Reader};
use std::{fs, path::Path};

pub struct Geo {
    reader: geoip2::Reader<'static, City<'static>>,
}

impl Geo {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let buffer = Box::new(fs::read(path)?).leak();
        Ok(Self {
            reader: Reader::<City>::from_bytes(buffer)?,
        })
    }

    fn lookup(&self) {}
}

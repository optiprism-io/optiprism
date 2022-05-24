use std::path::Path;
use crate::error::{Result, Error};

pub struct Geo {
    pub country: Option<u16>,
    pub city: Option<u16>,
}

struct CSVGeo {
    country: Option<String>,
    city: Option<String>,
    weight: i32,
}

pub struct Device {
    device: Option<u16>,
    device_category: Option<u16>,
    os: Option<u16>,
    os_version: Option<u16>,
}

pub struct CSVDevice {
    device: Option<String>,
    device_category: Option<String>,
    os: Option<String>,
    os_version: Option<String>,
}

pub struct Profile {
    geo: Vec<Geo>,
    device: Vec<Device>,
}

impl Profile {
    pub fn try_new_from_csv<P: AsRef<Path>>(country_city_path: P, device_os_path: P) -> Result<Self> {
        let geo = {
            let mut rdr = csv::Reader::from_path(country_city_path)?;
            rdr.deserialize().into_iter().enumerate().map(|(id, p)| {
                match p {
                    Ok(v) => {
                        let rec: CSVGeo = v;

                        Ok(Geo {
                            country: rec.country,
                            city: rec.city,
                        })
                    }
                    Err(e) => Err(e)
                }
            }).collect::<csv::Result<Vec<Geo>>>()?
        };

        let device = {
            let mut rdr = csv::Reader::from_path(device_os_path)?;
            rdr.deserialize().into_iter().enumerate().map(|(id, p)| {
                match p {
                    Ok(v) => {
                        let rec: CSVDevice= v;

                        Ok(Device {
                            device: None,
                            device_category: None,
                            os: None,
                            os_version: None
                        })
                    }
                    Err(e) => Err(e)
                }
            }).collect::<csv::Result<Vec<Device>>>()?
        };

        Ok(Self {
            geo,
            device,
        })
    }
}

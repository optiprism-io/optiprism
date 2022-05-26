use std::path::Path;
use std::sync::Arc;
use rand::distributions::WeightedIndex;
use rand::rngs::ThreadRng;
use metadata::dictionaries;
use crate::error::{Result, Error};
use rand::prelude::*;

#[derive(Clone)]
pub struct Geo {
    pub country: Option<u16>,
    pub city: Option<u16>,
}

struct CSVGeo {
    country: Option<String>,
    city: Option<String>,
    weight: i32,
}

#[derive(Clone)]
pub struct Device {
    pub device: Option<u16>,
    pub device_category: Option<u16>,
    pub os: Option<u16>,
    pub os_version: Option<u16>,
}

pub struct CSVDevice {
    device: Option<String>,
    device_category: Option<String>,
    os: Option<String>,
    os_version: Option<String>,
    weight: i32,
}

pub struct Profile {
    pub geo: Geo,
    pub device: Device,
}

pub struct ProfileProvider {
    geo: Vec<Geo>,
    geo_weight_idx: WeightedIndex<i32>,
    device: Vec<Device>,
    device_weight_idx: WeightedIndex<i32>,
}

impl ProfileProvider {
    pub fn try_new_from_csv<P: AsRef<Path>>(org_id: u64, proj_id: u64, dicts: &Arc<dictionaries::Provider>, geo_path: P, device_path: P) -> Result<Self> {
        let mut geo_weights: Vec<i32> = Vec::with_capacity(1000);
        let geo = {
            let mut rdr = csv::Reader::from_path(geo_path)?;
            rdr.deserialize().into_iter().enumerate().map(|(id, p)| {
                match p {
                    Ok(v) => {
                        let rec: CSVGeo = v;
                        geo_weights.push(rec.weight);
                        Ok(Geo {
                            country: rec.country.and_then(|v| Some(dicts.get_key_or_create(org_id, proj_id, "user_country", v.as_str())?)),
                            city: rec.city.and_then(|v| Some(dicts.get_key_or_create(org_id, proj_id, "user_city", v.as_str())?)),
                        })
                    }
                    Err(e) => Err(e)
                }
            }).collect::<csv::Result<Vec<Geo>>>()?
        };
        geo_weights.shrink_to_fit();

        let mut device_weights: Vec<i32> = Vec::with_capacity(1000);
        let device = {
            let mut rdr = csv::Reader::from_path(device_path)?;
            rdr.deserialize().into_iter().enumerate().map(|(id, p)| {
                match p {
                    Ok(v) => {
                        let rec: CSVDevice = v;
                        device_weights.push(rec.weight);
                        Ok(Device {
                            device: rec.device.and_then(|v| Some(dicts.get_key_or_create(org_id, proj_id, "user_device", v.as_str())?)),
                            device_category: rec.device_category.and_then(|v| Some(dicts.get_key_or_create(org_id, proj_id, "user_device_category", v.as_str())?)),
                            os: rec.country.and_then(|v| Some(dicts.get_key_or_create(org_id, proj_id, "user_os", v.as_str())?)),
                            os_version: rec.country.and_then(|v| Some(dicts.get_key_or_create(org_id, proj_id, "user_os_version", v.as_str())?)),
                        })
                    }
                    Err(e) => Err(e)
                }
            }).collect::<csv::Result<Vec<Device>>>()?
        };
        device_weights.shrink_to_fit();

        Ok(Self {
            geo,
            geo_weight_idx: WeightedIndex::new(geo_weights).map_err(|err| Error::Internal(err.to_string()))?,
            device,
            device_weight_idx: WeightedIndex::new(device_weights).map_err(|err| Error::Internal(err.to_string()))?,
        })
    }

    pub fn sample(&mut self, rng: &mut ThreadRng) -> Profile {
        Profile {
            geo: self.geo[self.geo_weight_idx.sample(rng)].clone(),
            device: self.device[self.device_weight_idx.sample(rng)].clone(),
        }
    }
}

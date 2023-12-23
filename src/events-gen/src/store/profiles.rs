use std::io;
use std::sync::Arc;

use futures::executor::block_on;
use metadata::dictionaries;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand::rngs::ThreadRng;
use serde::Deserialize;
use tracing::info;
use common::types::{USER_PROPERTY_CITY, USER_PROPERTY_COUNTRY, USER_PROPERTY_DEVICE_MODEL, USER_PROPERTY_OS, USER_PROPERTY_OS_FAMILY, USER_PROPERTY_OS_VERSION_MAJOR};

use crate::error::EventsGenError;
use crate::error::Result;

#[derive(Clone)]
pub struct Geo {
    pub country: Option<u64>,
    pub city: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct CSVGeo {
    country: Option<String>,
    city: Option<String>,
    weight: i32,
}

#[derive(Clone)]
pub struct Device {
    pub device: Option<u64>,
    pub device_category: Option<u64>,
    pub os: Option<u64>,
    pub os_version: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
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
    pub fn try_new_from_csv<R: io::Read>(
        org_id: u64,
        proj_id: u64,
        dicts: &Arc<dyn dictionaries::Provider>,
        properties: &Arc<dyn metadata::properties::Provider>,
        geo_rdr: R,
        device_rdr: R,
    ) -> Result<Self> {
        let mut geo_weights: Vec<i32> = Vec::with_capacity(1000);
        info!("loading geo");
        let geo = {
            let mut result = Vec::with_capacity(1000);
            let mut rdr = csv::Reader::from_reader(geo_rdr);
            for res in rdr.deserialize() {
                let rec: CSVGeo = res?;
                geo_weights.push(rec.weight);
                let geo = Geo {
                    country: rec
                        .country
                        .map(|v| {
                            dicts.get_key_or_create(org_id, proj_id, properties.get_by_name(org_id, proj_id, USER_PROPERTY_COUNTRY).unwrap().column_name().as_str(), v.as_str())
                        })
                        .transpose()?,
                    city: rec
                        .city
                        .map(|v| dicts.get_key_or_create(org_id, proj_id, properties.get_by_name(org_id, proj_id, USER_PROPERTY_CITY).unwrap().column_name().as_str(), v.as_str()))
                        .transpose()?,
                };
                result.push(geo);
            }

            result.shrink_to_fit();

            result
        };
        geo_weights.shrink_to_fit();

        info!("loading devices");
        let mut device_weights: Vec<i32> = Vec::with_capacity(1000);
        let device = {
            let mut result = Vec::with_capacity(1000);
            let mut rdr = csv::Reader::from_reader(device_rdr);
            for res in rdr.deserialize() {
                let rec: CSVDevice = res?;
                device_weights.push(rec.weight);
                let device = Device {
                    device: rec
                        .device
                        .map(|v| {
                            dicts.get_key_or_create(org_id, proj_id, properties.get_by_name(org_id, proj_id, USER_PROPERTY_DEVICE_MODEL).unwrap().column_name().as_str(), v.as_str())
                        })
                        .transpose()?,
                    device_category: rec
                        .device_category
                        .map(|v| {
                            dicts.get_key_or_create(org_id, proj_id, properties.get_by_name(org_id, proj_id, USER_PROPERTY_OS_FAMILY).unwrap().column_name().as_str(), v.as_str())
                        })
                        .transpose()?,
                    os: rec
                        .os
                        .map(|v| dicts.get_key_or_create(org_id, proj_id, properties.get_by_name(org_id, proj_id, USER_PROPERTY_OS).unwrap().column_name().as_str(), v.as_str()))
                        .transpose()?,
                    os_version: rec
                        .os_version
                        .map(|v| {
                            dicts.get_key_or_create(org_id, proj_id, properties.get_by_name(org_id, proj_id, USER_PROPERTY_OS_VERSION_MAJOR).unwrap().column_name().as_str(), v.as_str())
                        })
                        .transpose()?,
                };

                result.push(device);
            }
            result.shrink_to_fit();

            result
        };
        device_weights.shrink_to_fit();

        Ok(Self {
            geo,
            geo_weight_idx: WeightedIndex::new(geo_weights)
                .map_err(|err| EventsGenError::Internal(err.to_string()))?,
            device,
            device_weight_idx: WeightedIndex::new(device_weights)
                .map_err(|err| EventsGenError::Internal(err.to_string()))?,
        })
    }

    pub fn sample(&mut self, rng: &mut ThreadRng) -> Profile {
        Profile {
            geo: self.geo[self.geo_weight_idx.sample(rng)].clone(),
            device: self.device[self.device_weight_idx.sample(rng)].clone(),
        }
    }
}

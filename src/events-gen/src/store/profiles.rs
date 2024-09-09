use std::io;
use std::net::IpAddr;

use fake::faker::internet::en::FreeEmail;
use fake::faker::internet::en::IP;
use fake::faker::name::en::FirstName;
use fake::faker::name::en::LastName;
use fake::Fake;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand::rngs::ThreadRng;
use serde::Deserialize;
use tracing::info;
use uuid::Uuid;

use crate::error::EventsGenError;
use crate::error::Result;
use crate::store::companies::Company;
use crate::store::companies::CompanyProvider;

#[derive(Clone)]
pub struct Geo {
    pub country: Option<String>,
    pub city: Option<String>,
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
    pub device: Option<String>,
    pub device_category: Option<String>,
    pub os: Option<String>,
    pub os_version: Option<String>,
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
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub anonymous_id: String,
    pub age: usize,
    pub ip: IpAddr,
    pub company: Company,
    pub project: String,
}

pub struct ProfileProvider {
    geo: Vec<Geo>,
    geo_weight_idx: WeightedIndex<i32>,
    device: Vec<Device>,
    device_weight_idx: WeightedIndex<i32>,
    companies: CompanyProvider,
}

impl ProfileProvider {
    pub fn try_new_from_csv<R: io::Read>(
        geo_rdr: R,
        device_rdr: R,
        companies: CompanyProvider,
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
                    country: rec.country.clone(),
                    city: rec.city.clone(),
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
                    device: rec.device.clone(),
                    device_category: rec.device_category.clone(),
                    os: rec.os.clone(),
                    os_version: rec.os_version.clone(),
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
            companies,
        })
    }

    pub fn sample(&mut self, rng: &mut ThreadRng) -> Profile {
        Profile {
            geo: self.geo[self.geo_weight_idx.sample(rng)].clone(),
            device: self.device[self.device_weight_idx.sample(rng)].clone(),
            first_name: FirstName().fake(),
            last_name: LastName().fake(),
            email: FreeEmail().fake(),
            anonymous_id: Uuid::new_v4().to_string(),
            age: rng.gen_range(18..50),
            ip: IP().fake(),
            company: self.companies.sample(rng),
            project: format!("Project {}", rng.gen_range(1..=10)),
        }
    }
}

use std::io;

use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use serde::Deserialize;

use crate::error::Result;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct CSVCompany {
    pub name: String,
    pub staff: usize,
    pub age:usize,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Company {
    pub name: String,
    pub staff: usize,
    pub age: usize,
}

pub struct CompanyProvider {
    pub companies: Vec<Company>,
}

impl CompanyProvider {
    pub fn try_new_from_csv<R: io::Read>(rng: &mut ThreadRng, rdr: R) -> Result<Self> {
        let mut rdr = csv::Reader::from_reader(rdr);
        let mut companies = Vec::with_capacity(1000);
        for res in rdr.deserialize() {
            let rec: CSVCompany = res?;

            let company = Company {
                name: rec.name,
                staff: rec.staff,
                age: rec.age,
            };
            companies.push(company);
        }

        companies.shuffle(rng);

        Ok(Self { companies })
    }

    pub fn sample(&self, rng: &mut ThreadRng) -> Company {
        self.companies.choose(rng).unwrap().to_owned()
    }
}

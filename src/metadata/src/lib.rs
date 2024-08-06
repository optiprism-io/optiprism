#![feature(pattern)]

extern crate core;

pub mod accounts;
pub mod custom_events;
pub mod dashboards;
pub mod dictionaries;
pub mod error;
pub mod events;
pub mod groups;
pub mod index;
pub mod metadata;
pub mod organizations;
pub mod projects;
pub mod properties;
pub mod reports;
pub mod rocksdb;
pub mod sessions;
pub mod teams;
pub mod util;
pub mod bookmarks;
pub mod config;
pub mod backups;

use std::fmt::Debug;
use ::rocksdb::Transaction;
use ::rocksdb::TransactionDB;
use bincode::deserialize;
use datafusion::parquet::data_type::AsBytes;
pub use error::Result;
use serde::de::DeserializeOwned;
use crate::metadata::ListResponse;
pub use crate::metadata::MetadataProvider;
use crate::metadata::ResponseMetadata;

pub mod account {
    include!(concat!(env!("OUT_DIR"), "/account.rs"));
}

pub mod bookmark {
    include!(concat!(env!("OUT_DIR"), "/bookmark.rs"));
}

pub mod custom_event {
    include!(concat!(env!("OUT_DIR"), "/custom_event.rs"));
}

pub mod dashboard {
    include!(concat!(env!("OUT_DIR"), "/dashboard.rs"));
}

pub mod event {
    include!(concat!(env!("OUT_DIR"), "/event.rs"));
}

pub mod group {
    include!(concat!(env!("OUT_DIR"), "/group.rs"));
}

pub mod organization {
    include!(concat!(env!("OUT_DIR"), "/organization.rs"));
}

pub mod project {
    include!(concat!(env!("OUT_DIR"), "/project.rs"));
}

pub mod property {
    include!(concat!(env!("OUT_DIR"), "/property.rs"));
}

pub mod report {
    include!(concat!(env!("OUT_DIR"), "/report.rs"));
}

pub mod session {
    include!(concat!(env!("OUT_DIR"), "/session.rs"));
}

pub mod team {
    include!(concat!(env!("OUT_DIR"), "/team.rs"));
}

pub mod backup {
    include!(concat!(env!("OUT_DIR"), "/backup.rs"));
}

pub fn project_ns(project_id: u64, ns: &[u8]) -> Vec<u8> {
    [b"projects/", project_id.to_string().as_bytes(), b"/", ns].concat()
}

pub fn org_ns(organization_id: u64, ns: &[u8]) -> Vec<u8> {
    [
        b"organizations/",
        organization_id.to_string().as_bytes(),
        b"/",
        ns,
    ]
        .concat()
}

pub fn make_data_value_key(ns: &[u8], id: u64) -> Vec<u8> {
    [ns, b"/data/", id.to_string().as_bytes()].concat()
}

pub fn make_data_key(ns: &[u8]) -> Vec<u8> {
    [ns, b"/data"].concat()
}

pub fn make_index_key(ns: &[u8], idx_name: &[u8], key: &str) -> Vec<u8> {
    [ns, b"/idx/", idx_name, b"/", key.as_bytes()].concat()
}

pub fn make_id_seq_key(ns: &[u8]) -> Vec<u8> {
    [ns, b"/id_seq"].concat()
}

pub fn list_data<T>(tx: &Transaction<TransactionDB>, ns: &[u8]) -> Result<ListResponse<T>>
where
    T: DeserializeOwned + Debug,
{
    let prefix = make_data_key(ns);

    let list = tx
        .prefix_iterator("")
        .filter_map(|x| {
            let x = x.unwrap();
            if x.0.len() < prefix.len() || !prefix.as_slice().cmp(&x.0[..prefix.len()]).is_eq() {
                return None;
            }

            Some(deserialize(x.1.as_ref()))
        })
        .collect::<bincode::Result<_>>()?;

    Ok(ListResponse {
        data: list,
        meta: ResponseMetadata { next: None },
    })
}

pub fn list<T>(tx: &Transaction<TransactionDB>, path: &[u8]) -> Result<ListResponse<T>>
where
    T: DeserializeOwned + Debug,
{
    let prefix = path;

    let list = tx
        .prefix_iterator("")
        .filter_map(|x| {
            let x = x.unwrap();
            if x.0.len() < prefix.len() || !prefix.cmp(&x.0[..prefix.len()]).is_eq() {
                return None;
            }

            Some(deserialize(x.1.as_ref()))
        })
        .collect::<bincode::Result<_>>()?;

    Ok(ListResponse {
        data: list,
        meta: ResponseMetadata { next: None },
    })
}

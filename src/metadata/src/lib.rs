pub mod accounts;
pub mod custom_events;
pub mod dashboards;
pub mod dictionaries;
pub mod error;
pub mod events;
pub mod index;
pub mod metadata;
pub mod organizations;
pub mod projects;
pub mod properties;
pub mod reports;
pub mod rocksdb;
pub mod teams;
pub mod test_util;

use ::rocksdb::Transaction;
use ::rocksdb::TransactionDB;
use bincode::deserialize;
pub use error::Result;
use serde::de::DeserializeOwned;

use crate::metadata::ListResponse;
pub use crate::metadata::MetadataProvider;
use crate::metadata::ResponseMetadata;

pub fn org_proj_ns(organization_id: u64, project_id: u64, ns: &[u8]) -> Vec<u8> {
    [
        b"organizations/",
        organization_id.to_le_bytes().as_ref(),
        b"/projects/",
        project_id.to_le_bytes().as_ref(),
        b"/",
        ns,
    ]
    .concat()
}

pub fn org_ns(organization_id: u64, ns: &[u8]) -> Vec<u8> {
    [
        b"organizations/",
        organization_id.to_le_bytes().as_ref(),
        b"/",
        ns,
    ]
    .concat()
}

pub fn make_data_value_key(ns: &[u8], id: u64) -> Vec<u8> {
    [ns, b"/data/", id.to_le_bytes().as_ref()].concat()
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

pub fn list<T>(tx: &Transaction<TransactionDB>, ns: &[u8]) -> Result<ListResponse<T>>
where T: DeserializeOwned {
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

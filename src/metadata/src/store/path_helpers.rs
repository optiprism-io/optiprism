use std::sync::Arc;

use bincode::deserialize;
use rocksdb::Transaction;
use rocksdb::TransactionDB;
use serde::de::DeserializeOwned;

use crate::metadata::ListResponse;
use crate::metadata::ResponseMetadata;
use crate::store::Store;
use crate::Result;

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

pub fn list<'a, T>(tx: &Transaction<TransactionDB>, ns: &[u8]) -> Result<ListResponse<T>>
where T: DeserializeOwned {
    let prefix = make_data_key(ns);

    let list = store
        .list_prefix("")?
        .iter()
        .filter_map(|x| {
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

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Scope {
    Organization,
    Project(u64),
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum Permission {
    AccountGetById,
    List,
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum Role {
    Owner,
    Manager,
    Reader,
}

pub const MANAGER_PERMISSIONS: [Permission; 1] = [Permission::AccountGetById];

pub const READER_PERMISSIONS: [Permission; 1] = [Permission::AccountGetById];

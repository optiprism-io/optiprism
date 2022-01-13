use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Scope {
    Organization,
    Project(u64),
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum Permission {
    AccountCreate,
    AccountGetById,
    AccountList,
    AccountUpdate,
    AccountDelete,
}

#[derive(Serialize, Deserialize, PartialEq)]
pub enum Role {
    Owner,
    Manager,
    Reader,
}

pub const MANAGER_PERMISSIONS: [Permission; 5] = [
    Permission::AccountCreate,
    Permission::AccountGetById,
    Permission::AccountList,
    Permission::AccountUpdate,
    Permission::AccountDelete,
];

pub const READER_PERMISSIONS: [Permission; 2] =
    [Permission::AccountGetById, Permission::AccountList];

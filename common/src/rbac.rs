use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub enum Scope {
    Organization,
    Project(u64),
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub enum Permission {
    CreateAccount,
    GetAccountById,
    UpdateAccount,
    DeleteAccount,
    ListAccounts,

    CreateEvent,
    GetEventById,
    GetEventByName,
    UpdateEvent,
    DeleteEvent,
    ListEvents,
    AttachPropertyToEvent,
    DetachPropertyFromEvent
}

#[derive(Serialize, Deserialize, PartialEq, Clone)]
pub enum Role {
    Owner,
    Manager,
    Reader,
}

pub enum Resource {
    Account,
    Project,
    Organization,
    Event,
    EventProperty,
}

pub enum Action {
    Read,
    Modify,
    Write,
    Delete,
}

pub const MANAGER_PERMISSIONS: [Permission; 5] = [
    Permission::CreateAccount,
    Permission::GetAccountById,
    Permission::ListAccounts,
    Permission::UpdateAccount,
    Permission::DeleteAccount,
];

pub const READER_PERMISSIONS: [Permission; 2] =
    [Permission::GetAccountById, Permission::ListAccounts];

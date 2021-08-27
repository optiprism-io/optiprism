use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Scope {
    Organization,
    Project(u64),
}

#[derive(Serialize, Deserialize)]
pub enum Role {
    Owner,
    Manager,
    Reader,
}

#[derive(Serialize, Deserialize)]
pub enum Permission {
    List,
}

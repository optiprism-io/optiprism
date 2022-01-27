use common::rbac::{Permission, Role};
use serde::Deserialize;
use std::collections::HashMap;
use metadata::events::{Scope, Status};

#[derive(Deserialize)]
pub struct CreateRequest {
    pub created_by: u64,
    pub tags: Vec<String>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub scope: Scope,
    pub properties: Option<Vec<u64>>,
    pub global_properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}

#[derive(Deserialize)]
pub struct UpdateRequest {
    pub created_by: u64,
    pub updated_by: u64,
    pub tags: Vec<String>,
    pub name: String,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub status: Status,
    pub scope: Scope,
    pub properties: Option<Vec<u64>>,
    pub global_properties: Option<Vec<u64>>,
    pub custom_properties: Option<Vec<u64>>,
}
use crate::{
    rbac::{Permission, Role, Scope},
};
use crate::error::Result;

use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256, Sha3_512};
use std::{collections::HashMap, env::var};

lazy_static::lazy_static! {
    pub static ref COMMON_SALT: String = var("FNP_COMMON_SALT").unwrap();
    pub static ref EMAIL_TOKEN_KEY: String = var("FNP_EMAIL_TOKEN_KEY").unwrap();
    pub static ref ACCESS_TOKEN_KEY: String = var("FNP_ACCESS_TOKEN_KEY").unwrap();
    pub static ref REFRESH_TOKEN_KEY: String = var("FNP_REFRESH_TOKEN_KEY").unwrap();

    pub static ref ACCESS_TOKEN_DURATION: Duration = Duration::hours(1);
    pub static ref REFRESH_TOKEN_DURATION: Duration = Duration::days(30);
}

#[derive(Serialize, Deserialize)]
pub struct AccessClaims {
    pub exp: i64,
    pub organization_id: u64,
    pub account_id: u64,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
}

#[derive(Serialize, Deserialize)]
pub struct RefreshClaims {
    pub organization_id: u64,
    pub account_id: u64,
    pub exp: i64,
}

pub fn make_token<T: Serialize>(claims: T, key: &[u8]) -> Result<String> {
    let header = Header {
        alg: Algorithm::HS512,
        ..Default::default()
    };
    Ok(encode(&header, &claims, &EncodingKey::from_secret(key))?)
}

pub fn parse_access_token(value: &str) -> Result<AccessClaims> {
    let token = decode(
        value,
        &DecodingKey::from_secret(ACCESS_TOKEN_KEY.as_bytes()),
        &Validation::new(Algorithm::HS512),
    )?;
    Ok(token.claims)
}

pub fn parse_refresh_token(value: &str) -> Result<RefreshClaims> {
    let token = decode(
        value,
        &DecodingKey::from_secret(REFRESH_TOKEN_KEY.as_bytes()),
        &Validation::new(Algorithm::HS512),
    )?;
    Ok(token.claims)
}

pub fn is_valid_password(password: &str, salt: &str, password_hash: &str) -> bool {
    password_hash == make_password_hash(password, salt)
}

pub fn make_salt() -> String {
    let rand_part: String = thread_rng()
        .sample_iter(&Alphanumeric)
        .take(64)
        .map(char::from)
        .collect();
    let mut salt: String = Utc::now().to_rfc3339();
    salt.push_str(&rand_part);
    let mut hasher = Sha3_256::new();
    hasher.update(salt);
    hex::encode(hasher.finalize())
}

pub fn make_password_hash(password: &str, salt: &str) -> String {
    let mut hasher = Sha3_512::new();
    hasher.update(vec![password, salt, &COMMON_SALT].concat());
    hex::encode(hasher.finalize())
}

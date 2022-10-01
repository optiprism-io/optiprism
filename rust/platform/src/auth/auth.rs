use crate::error::Result;

use chrono::{DateTime, Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env::var};
use argon2::Argon2;
use password_hash::PasswordHash;
use metadata::accounts::Account;
use crate::auth::types::TokenResponse;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[serde_with::serde_as]
pub struct AccessClaims {
    pub exp: i64,
    pub account_id: u64,
}

pub fn make_token<T: Serialize>(claims: T, key: impl AsRef<[u8]>) -> Result<String> {
    let header = Header {
        alg: Algorithm::HS512,
        ..Default::default()
    };
    Ok(encode(&header, &claims, &EncodingKey::from_secret(key.as_ref()))?)
}

pub fn make_access_token(expires: Duration, account_id: u64, token_key: impl AsRef<[u8]>) -> Result<String> {
    Ok(make_token(
        AccessClaims {
            exp: (Utc::now() + expires).timestamp(),
            account_id,
        },
        token_key,
    )?)
}

pub fn parse_access_token(value: &str, token_key: impl AsRef<[u8]>) -> Result<AccessClaims> {
    let token = decode(
        value,
        &DecodingKey::from_secret(token_key.as_ref()),
        &Validation::new(Algorithm::HS512),
    )?;
    Ok(token.claims)
}

pub fn make_password_hash(password: &str) -> Result<String> {
    let salt = password_hash::SaltString::generate(rand::thread_rng());
    let hash = password_hash::PasswordHash::generate(
        argon2::Argon2::new(
            argon2::Algorithm::Argon2d,
            argon2::Version::V0x10,
            argon2::Params::default(),
        ),
        password,
        salt.as_str(),
    )?;

    Ok(hash.to_string())
}

pub fn verify_password(password: impl AsRef<[u8]>, password_hash: PasswordHash) -> Result<()> {
    Ok(password_hash.verify_password(&[&Argon2::default()], password)?)
}
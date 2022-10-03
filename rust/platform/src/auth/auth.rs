use crate::error::Result;

use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};

use serde::{Deserialize, Serialize};

use argon2::Argon2;
use password_hash::PasswordHash;



#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[serde_with::serde_as]
pub struct AccessClaims {
    pub exp: i64,
    pub account_id: u64,
}


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[serde_with::serde_as]
pub struct RefreshClaims {
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

pub fn make_access_token(account_id: u64, expires: Duration, token_key: impl AsRef<[u8]>) -> Result<String> {
    Ok(make_token(
        AccessClaims {
            exp: (Utc::now() + expires).timestamp(),
            account_id,
        },
        token_key,
    )?)
}

pub fn make_refresh_token(account_id: u64, expires: Duration, token_key: impl AsRef<[u8]>) -> Result<String> {
    Ok(make_token(
        RefreshClaims {
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

pub fn parse_refresh_token(value: &str, token_key: impl AsRef<[u8]>) -> Result<RefreshClaims> {
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
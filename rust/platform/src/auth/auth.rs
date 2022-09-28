use crate::error::Result;

use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env::var};
use argon2::Argon2;
use password_hash::PasswordHash;
use metadata::accounts::Account;

#[derive(Serialize, Deserialize)]
pub struct AccessClaims {
    pub exp: i64,
    pub account_id: u64,
}

pub fn make_token<T: Serialize>(claims: T, key: impl AsRef<[u8]>) -> Result<String> {
    let header = Header {
        alg: Algorithm::HS512,
        ..Default::default()
    };
    Ok(encode(&header, &claims, &EncodingKey::from_secret(key))?)
}

pub fn make_access_token(expires: Duration, account_id:u64,token_key: impl AsRef<[u8]>) -> Result<String> {
    Ok(make_token(
        AccessClaims {
            exp: Utc::now().add(expires).timestamp(),
            account_id,
        },
        token_key.as_bytes(),
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

fn make_token_response(
    account: &Account,
    access_token_duration: Duration,
    access_token_key: impl AsRef<[u8]>,
) -> Result<String> {
    Ok(TokenResponse {
        access_token: make_token(
            AccessClaims {
                exp: Utc::now().add(access_token_duration).timestamp(),
                account_id,
            },
            access_token_key,
        )?
    })
}

pub fn make_password_hash(password: &str) -> Result<PasswordHash> {
    let salt = password_hash::SaltString::generate(rand::thread_rng());
    Ok(password_hash::PasswordHash::generate(
        argon2::Argon2::new(
            argon2::Algorithm::Argon2d,
            argon2::Version::V0x10,
            argon2::Params::default(),
        ),
        password,
        salt.as_str(),
    )?)
}

pub fn verify_password(password: impl AsRef<[u8]>, password_hash: PasswordHash) -> Result<()> {
    Ok(password_hash.verify_password(&[Argon2::default()], password)?)
}
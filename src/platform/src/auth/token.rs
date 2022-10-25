use chrono::Duration;
use chrono::Utc;
use jsonwebtoken::decode;
use jsonwebtoken::encode;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use jsonwebtoken::Validation;
use serde::Deserialize;
use serde::Serialize;

use crate::error::Result;

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
    Ok(encode(
        &header,
        &claims,
        &EncodingKey::from_secret(key.as_ref()),
    )?)
}

pub fn make_access_token(
    account_id: u64,
    expires: Duration,
    token_key: impl AsRef<[u8]>,
) -> Result<String> {
    make_token(
        AccessClaims {
            exp: (Utc::now() + expires).timestamp(),
            account_id,
        },
        token_key,
    )
}

pub fn make_refresh_token(
    account_id: u64,
    expires: Duration,
    token_key: impl AsRef<[u8]>,
) -> Result<String> {
    make_token(
        RefreshClaims {
            exp: (Utc::now() + expires).timestamp(),
            account_id,
        },
        token_key,
    )
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

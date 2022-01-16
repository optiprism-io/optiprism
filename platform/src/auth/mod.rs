pub mod provider;
pub mod types;

use crate::error::Result;
use chrono::Duration;
use std::env::var;
use types::AccessClaims;

lazy_static::lazy_static! {
    pub static ref COMMON_SALT: String = var("FNP_COMMON_SALT").unwrap();
    pub static ref EMAIL_TOKEN_KEY: String = var("FNP_EMAIL_TOKEN_KEY").unwrap();
    pub static ref ACCESS_TOKEN_KEY: String = var("FNP_ACCESS_TOKEN_KEY").unwrap();
    pub static ref REFRESH_TOKEN_KEY: String = var("FNP_REFRESH_TOKEN_KEY").unwrap();

    pub static ref ACCESS_TOKEN_DURATION: Duration = Duration::hours(1);
    pub static ref REFRESH_TOKEN_DURATION: Duration = Duration::days(30);
}

pub fn parse_access_token(value: &str) -> Result<AccessClaims> {
    let token = jsonwebtoken::decode(
        value,
        &jsonwebtoken::DecodingKey::from_secret(ACCESS_TOKEN_KEY.as_bytes()),
        &jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS512),
    )?;
    Ok(token.claims)
}

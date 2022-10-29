use serde::Deserialize;
use serde::Serialize;
use validator::validate_email;

use crate::error::Result;
use crate::error::ValidationError;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignUpRequest {
    pub email: String,
    pub password: String,
    pub password_repeat: String,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
}

impl SignUpRequest {
    pub fn validate(&self) -> Result<()> {
        let mut res_err = ValidationError::new();
        if !validate_email(&self.email) {
            res_err.push_invalid("email")
        }

        match zxcvbn::zxcvbn(&self.password, &[&self.email]) {
            Ok(ent) if ent.score() < 3 => res_err.push("password", "password is too simple"),
            Err(err) => res_err.push("password", err.to_string()),
            _ => {}
        }

        res_err.result()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogInRequest {
    pub email: String,
    pub password: String,
}

impl LogInRequest {
    pub fn validate(&self) -> Result<()> {
        let mut err = ValidationError::new();
        if !validate_email(&self.email) {
            err.push_invalid("email")
        }

        err.result()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RecoverPasswordRequest {
    pub email: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TokensResponse {
    pub access_token: String,
    pub refresh_token: String,
}

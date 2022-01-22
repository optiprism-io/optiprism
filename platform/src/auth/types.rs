use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct LogInRequest {
    pub email: String,
    pub password: String,
}

#[derive(Deserialize)]
pub struct SignUpRequest {
    pub organization_name: String,
    pub email: String,
    pub password: String,
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
}

#[derive(Deserialize)]
pub struct RefreshRequest {
    pub refresh_token: String,
}

#[derive(Deserialize)]
pub struct RecoverRequest {
    pub email: String,
}

#[derive(Serialize)]
pub struct TokensResponse {
    pub access_token: String,
    pub refresh_token: String,
}

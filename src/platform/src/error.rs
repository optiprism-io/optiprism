use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::result;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::Json;
use common::error::CommonError;
use metadata::error::AccountError;
use metadata::error::CustomEventError;
use metadata::error::DatabaseError;
use metadata::error::DictionaryError;
use metadata::error::EventError;
use metadata::error::MetadataError;
use metadata::error::OrganizationError;
use metadata::error::ProjectError;
use metadata::error::PropertyError;
use metadata::error::StoreError;
use metadata::error::TeamError;
use query::error::QueryError;
use serde::Serialize;
use serde::Serializer;
use thiserror::Error;

pub type Result<T> = result::Result<T, PlatformError>;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("invalid refresh token")]
    InvalidRefreshToken,
    #[error("password hashing error")]
    InvalidPasswordHashing,
    #[error("can't make access token")]
    CantMakeAccessToken,
    #[error("can't make refresh token")]
    CantMakeRefreshToken,
    #[error("can't parse bearer header")]
    CantParseBearerHeader,
    #[error("can't parse access token")]
    CantParseAccessToken,
}

impl Display for ApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message.clone().unwrap_or_default())
    }
}

#[derive(Error, Debug)]
pub enum PlatformError {
    #[error("{1:?} error wrapped into {0:?}")]
    Wrapped(Box<PlatformError>, Box<PlatformError>),
    #[error("invalid fields")]
    InvalidFields(BTreeMap<String, String>),
    #[error("bad request: {0:?}")]
    BadRequest(String),
    #[error("unauthorized: {0:?}")]
    Unauthorized(String),
    #[error("forbidden: {0:?}")]
    Forbidden(String),
    #[error("not found: {0:?}")]
    NotFound(String),
    #[error("internal: {0:?}")]
    Internal(String),
    #[error("serde: {0:?}")]
    Serde(#[from] serde_json::Error),
    #[error("password hash")]
    PasswordHash(#[from] password_hash::Error),
    #[error("jsonwebtoken: {0:?}")]
    JSONWebToken(#[from] jsonwebtoken::errors::Error),
    #[error("decimal: {0:?}")]
    Decimal(#[from] rust_decimal::Error),
    #[error("metadata: {0:?}")]
    Metadata(#[from] MetadataError),
    #[error("query: {0:?}")]
    Query(#[from] QueryError),
    #[error("common: {0:?}")]
    Common(#[from] CommonError),
    #[error("session: {0:?}")]
    Auth(#[from] AuthError),
    #[error("axum: {0:?}")]
    Axum(#[from] axum::http::Error),
    #[error("hyper: {0:?}")]
    Hyper(#[from] hyper::Error),
    #[error("other: {0:?}")]
    Other(#[from] anyhow::Error),
}

impl PlatformError {
    pub fn wrap_into(self, err: impl Into<PlatformError>) -> PlatformError {
        PlatformError::Wrapped(Box::new(self), Box::new(err.into()))
    }

    pub fn into_api_error(self) -> ApiError {
        match self {
            PlatformError::Serde(err) => ApiError::bad_request(err.to_string()),
            PlatformError::Decimal(err) => ApiError::bad_request(err.to_string()),
            PlatformError::Metadata(err) => match err {
                MetadataError::Database(err) => match err {
                    DatabaseError::ColumnAlreadyExists(_) => ApiError::conflict(err.to_string()),
                    DatabaseError::TableNotFound(_) => ApiError::not_found(err.to_string()),
                    DatabaseError::TableAlreadyExists(_) => ApiError::conflict(err.to_string()),
                },
                MetadataError::Account(err) => match err {
                    AccountError::AccountNotFound(_) => ApiError::not_found(err.to_string()),
                    AccountError::AccountAlreadyExist(_) => ApiError::conflict(err.to_string()),
                },
                MetadataError::Organization(err) => match err {
                    OrganizationError::OrganizationNotFound(_) => {
                        ApiError::not_found(err.to_string())
                    }
                    OrganizationError::OrganizationAlreadyExist(_) => {
                        ApiError::conflict(err.to_string())
                    }
                },
                MetadataError::Project(err) => match err {
                    ProjectError::ProjectNotFound(_) => ApiError::not_found(err.to_string()),
                    ProjectError::ProjectAlreadyExist(_) => ApiError::conflict(err.to_string()),
                },
                MetadataError::Event(err) => match err {
                    EventError::EventNotFound(_) => ApiError::not_found(err.to_string()),
                    EventError::EventAlreadyExist(_) => ApiError::conflict(err.to_string()),
                    EventError::PropertyNotFound(_) => ApiError::not_found(err.to_string()),
                    EventError::PropertyAlreadyExist(_) => ApiError::conflict(err.to_string()),
                },
                MetadataError::Property(err) => match err {
                    PropertyError::PropertyNotFound(_) => ApiError::not_found(err.to_string()),
                    PropertyError::PropertyAlreadyExist(_) => ApiError::conflict(err.to_string()),
                },
                MetadataError::Dictionary(err) => match err {
                    DictionaryError::KeyNotFound(_) => ApiError::not_found(err.to_string()),
                    DictionaryError::ValueNotFound(_) => ApiError::not_found(err.to_string()),
                },
                MetadataError::Store(err) => match err {
                    StoreError::KeyAlreadyExists(_) => ApiError::conflict(err.to_string()),
                    StoreError::KeyNotFound(_) => ApiError::not_found(err.to_string()),
                },
                MetadataError::RocksDb(err) => ApiError::internal(err.to_string()),
                MetadataError::FromUtf8(err) => ApiError::internal(err.to_string()),
                MetadataError::Bincode(err) => ApiError::internal(err.to_string()),
                MetadataError::Io(err) => ApiError::internal(err.to_string()),
                MetadataError::Other(_) => ApiError::internal(err.to_string()),
                MetadataError::CustomEvent(err) => match err {
                    CustomEventError::EventNotFound(_) => ApiError::not_found(err.to_string()),
                    CustomEventError::EventAlreadyExist(_) => ApiError::conflict(err.to_string()),
                    CustomEventError::RecursionLevelExceeded(_) => {
                        ApiError::bad_request(err.to_string())
                    }
                    CustomEventError::DuplicateEvent => ApiError::conflict(err.to_string()),
                    CustomEventError::EmptyEvents => ApiError::bad_request(err.to_string()),
                },
                MetadataError::Team(err) => match err {
                    TeamError::TeamNotFound(_) => ApiError::not_found(err.to_string()),
                    TeamError::TeamAlreadyExist(_) => ApiError::conflict(err.to_string()),
                },
            },
            PlatformError::Query(err) => match err {
                QueryError::Internal(err) => ApiError::internal(err),
                QueryError::Plan(err) => ApiError::internal(err),
                QueryError::Execution(err) => ApiError::internal(err),
                QueryError::DataFusion(err) => ApiError::internal(err.to_string()),
                QueryError::Arrow(err) => ApiError::internal(err.to_string()),
                QueryError::Metadata(err) => ApiError::internal(err.to_string()),
            },
            PlatformError::BadRequest(msg) => ApiError::bad_request(msg),
            PlatformError::Internal(msg) => ApiError::internal(msg),
            PlatformError::Common(err) => match err {
                CommonError::DataFusionError(err) => ApiError::internal(err.to_string()),
                CommonError::JWTError(err) => ApiError::internal(err.to_string()),
                CommonError::EntityMapping => ApiError::internal(err.to_string()),
            },
            PlatformError::Auth(err) => match err {
                AuthError::InvalidCredentials => ApiError::unauthorized(err),
                AuthError::InvalidRefreshToken => ApiError::unauthorized(err),
                AuthError::InvalidPasswordHashing => ApiError::internal(err),
                AuthError::CantMakeAccessToken => ApiError::internal(err),
                AuthError::CantMakeRefreshToken => ApiError::internal(err),
                AuthError::CantParseBearerHeader => ApiError::unauthorized(err),
                AuthError::CantParseAccessToken => ApiError::unauthorized(err),
            },
            PlatformError::Unauthorized(err) => ApiError::unauthorized(err),
            PlatformError::Forbidden(err) => ApiError::forbidden(err),
            PlatformError::PasswordHash(err) => ApiError::internal(err.to_string()),
            PlatformError::JSONWebToken(err) => ApiError::internal(err.to_string()),
            PlatformError::Axum(err) => ApiError::internal(err.to_string()),
            PlatformError::Other(err) => ApiError::internal(err.to_string()),
            PlatformError::Hyper(err) => ApiError::internal(err.to_string()),
            PlatformError::Wrapped(_, outer) => outer.into_api_error(),
            PlatformError::InvalidFields(fields) => {
                ApiError::new(StatusCode::BAD_REQUEST).with_fields(fields)
            }
            PlatformError::NotFound(err) => ApiError::not_found(err),
        }
    }
}

#[derive(Error, Serialize, Debug, Clone)]
pub struct ApiError {
    #[serde(serialize_with = "serialize_http_code")]
    pub status: StatusCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiErrorWrapper {
    pub error: ApiError,
}

#[derive(Default)]
pub struct ValidationError {
    fields: BTreeMap<String, String>,
    _message: Option<String>,
}

impl ValidationError {
    pub fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
            _message: None,
        }
    }

    pub fn push(&mut self, field: impl Into<String>, err: impl Into<String>) {
        self.fields.insert(field.into(), err.into());
    }

    pub fn push_invalid(&mut self, field: impl Into<String>) {
        self.fields
            .insert(field.into(), "invalid field value".into());
    }

    pub fn result(self) -> Result<()> {
        if self.fields.is_empty() {
            Ok(())
        } else {
            Err(PlatformError::InvalidFields(self.fields))
        }
    }
}

pub fn serialize_http_code<S: Serializer>(
    status: &StatusCode,
    ser: S,
) -> std::result::Result<S::Ok, S::Error> {
    ser.serialize_u16(status.as_u16())
}

impl ApiError {
    pub fn bad_request(err: impl ToString) -> Self {
        ApiError::new(StatusCode::BAD_REQUEST).with_message(err.to_string())
    }

    pub fn forbidden(err: impl ToString) -> Self {
        ApiError::new(StatusCode::FORBIDDEN).with_message(err.to_string())
    }

    pub fn unauthorized(err: impl ToString) -> Self {
        ApiError::new(StatusCode::UNAUTHORIZED).with_message(err.to_string())
    }

    pub fn conflict(err: impl ToString) -> Self {
        ApiError::new(StatusCode::CONFLICT).with_message(err.to_string())
    }

    pub fn not_found(err: impl ToString) -> Self {
        ApiError::new(StatusCode::NOT_FOUND).with_message(err.to_string())
    }

    pub fn internal(err: impl ToString) -> Self {
        ApiError::new(StatusCode::INTERNAL_SERVER_ERROR).with_message(err.to_string())
    }

    pub fn new(status: StatusCode) -> Self {
        Self {
            status,
            code: None,
            message: None,
            fields: BTreeMap::new(),
        }
    }

    pub fn with_fields(self, fields: BTreeMap<String, String>) -> Self {
        Self {
            status: self.status,
            code: self.code,
            message: self.message,
            fields,
        }
    }

    pub fn with_message(self, message: String) -> Self {
        Self {
            status: self.status,
            code: self.code,
            message: Some(message),
            fields: self.fields,
        }
    }

    pub fn append_inner_message(self, inner: String) -> Self {
        Self {
            status: self.status,
            code: self.code,
            message: self.message.map(|msg| format!("{}: {}", msg, inner)),
            fields: self.fields,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(ApiErrorWrapper { error: self })).into_response()
    }
}

impl IntoResponse for PlatformError {
    fn into_response(self) -> Response {
        self.into_api_error().into_response()
    }
}

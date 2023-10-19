use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;

use async_trait::async_trait;
use axum::body::HttpBody;
use axum::extract::rejection::JsonRejection;
use axum::extract::FromRequest;
use axum::http;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum_core::body;
use axum_core::response::Response;
use axum_core::BoxError;
use bytes::Bytes;
use hyper::Body;
use lazy_static::lazy_static;
use log::debug;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde::Serializer;
use serde_json::Value;
use thiserror::Error;

use crate::error::CommonError;
use crate::error::Result;
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

impl Display for ApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message.clone().unwrap_or_default())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ApiErrorWrapper {
    pub error: ApiError,
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
            message: self.message.map(|msg| format!("{msg}: {inner}")),
            fields: self.fields,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        debug!("ApiError: {:?}", self);
        (self.status, Json(ApiErrorWrapper { error: self })).into_response()
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Json<T>(pub T);

#[async_trait]
impl<T, S, B> FromRequest<S, B> for Json<T>
where
    T: DeserializeOwned,
    B: HttpBody + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(
        req: Request<B>,
        state: &S,
    ) -> std::result::Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req, state).await {
            Ok(v) => Ok(Json(v.0)),
            Err(err) => {
                let mut api_err = ApiError::bad_request(err.to_string());

                if let Some(inner) = err.source() {
                    if let Some(inner) = inner.source() {
                        api_err = api_err.append_inner_message(inner.to_string());
                        match err {
                            JsonRejection::JsonDataError(_) => {
                                lazy_static! {
                                    static ref FIELD_RX: Regex =
                                        Regex::new(r"(\w+?) field `(.+?)`").unwrap();
                                }
                                if let Some(captures) =
                                    FIELD_RX.captures(inner.to_string().as_str())
                                {
                                    api_err = api_err.with_fields(BTreeMap::from([(
                                        captures[2].to_string(),
                                        captures[1].to_string(),
                                    )]));
                                }
                            }
                            JsonRejection::JsonSyntaxError(_) => {}
                            JsonRejection::MissingJsonContentType(_) => {}
                            JsonRejection::BytesRejection(_) => {}
                            _ => unreachable!(),
                        }
                    }
                }

                Err(api_err)
            }
        }
    }
}

impl<T> IntoResponse for Json<T>
where T: Serialize
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

fn content_length(headers: &HeaderMap<HeaderValue>) -> Option<u64> {
    headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok())
}

pub async fn print_request_response(
    mut req: Request<Body>,
    next: Next<Body>,
) -> Result<impl IntoResponse> {
    tracing::debug!("request headers = {:?}", req.headers());

    if content_length(req.headers()).is_some() {
        let (parts, body) = req.into_parts();
        let bytes = buffer_and_print("request", body).await?;
        req = Request::from_parts(parts, Body::from(bytes));
    }

    let res = next.run(req).await;
    if content_length(res.headers()).is_none() {
        return Ok(res);
    }

    let (parts, body) = res.into_parts();
    let bytes = buffer_and_print("response", body).await?;

    Ok(Response::from_parts(parts, body::boxed(Body::from(bytes))))
}

async fn buffer_and_print<B>(direction: &str, body: B) -> Result<Bytes>
where
    B: HttpBody<Data = Bytes>,
    B::Error: std::fmt::Display,
{
    let bytes = match hyper::body::to_bytes(body).await {
        Ok(bytes) => bytes,
        Err(err) => {
            return Err(CommonError::BadRequest(format!(
                "failed to read {direction} body: {err}"
            )));
        }
    };

    if let Ok(body) = std::str::from_utf8(&bytes) {
        let v = serde_json::from_slice::<Value>(body.as_bytes())?;
        tracing::debug!("{} body = {}", direction, serde_json::to_string_pretty(&v)?);
    }

    Ok(bytes)
}
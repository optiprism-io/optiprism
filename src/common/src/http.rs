use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Instant;
use async_trait::async_trait;
use axum::body::HttpBody;
use axum::extract::rejection::JsonRejection;
use axum::extract::FromRequest;
use axum::http;
use axum::http::{HeaderMap, Method};
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::middleware::{self};
use axum::response::Html;
use axum::response::IntoResponse;
use axum::routing::post;
use axum::Router;
use axum_core::body;
use axum_core::body::Body;
use axum_core::extract::Request;
use axum_core::response::Response;
use axum_core::BoxError;
use bytes::Bytes;
use http_body_util::BodyExt;
use lazy_static::lazy_static;
use log::debug;
use metrics::{counter, histogram};
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde::Serializer;
use serde_json::Value;
use thiserror::Error;

use crate::error::CommonError;
use crate::error::Result;
use crate::types::{METRIC_HTTP_REQUEST_TIME_SECONDS, METRIC_HTTP_REQUESTS_TOTAL};

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
    pub fn unimplemented(err: impl ToString) -> Self {
        ApiError::new(StatusCode::NOT_IMPLEMENTED).with_message(err.to_string())
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
impl<T, S> FromRequest<S> for Json<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> std::result::Result<Self, Self::Rejection> {
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
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

pub fn content_length(headers: &HeaderMap<HeaderValue>) -> Option<u64> {
    headers
        .get(http::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok()?.parse::<u64>().ok())
}

pub async fn measure_request_response(
    req: Request,
    next: Next,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    let start = Instant::now();
    let path = req.uri().path().to_string();
    let metrics = req.method() != Method::OPTIONS;
    let res = next.run(req).await;
    if metrics {
        histogram!(METRIC_HTTP_REQUEST_TIME_SECONDS,"path"=>path.to_owned(),"status"=>res.status().as_u16().to_string()).record(start.elapsed().as_millis() as f64);
        counter!(METRIC_HTTP_REQUESTS_TOTAL,"path"=>path,"status"=>res.status().as_u16().to_string()).increment(1);
    }
    Ok(res)
}


pub async fn print_request_response(
    req: Request,
    next: Next,
) -> std::result::Result<impl IntoResponse, (StatusCode, String)> {
    tracing::debug!("{} {}", req.method(), req.uri());
    let (parts, body) = req.into_parts();
    let bytes = buffer_and_print("request", body).await?;
    let req = Request::from_parts(parts, Body::from(bytes));

    let res = next.run(req).await;

    Ok(res)
}

async fn buffer_and_print<B>(
    direction: &str,
    body: B,
) -> std::result::Result<Bytes, (StatusCode, String)>
where
    B: axum::body::HttpBody<Data=Bytes>,
    B::Error: std::fmt::Display,
{
    let bytes = match body.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("failed to read {direction} body: {err}"),
            ));
        }
    };

    if let Ok(body) = std::str::from_utf8(&bytes) {
        tracing::debug!("{direction} body = {body}");
    }

    Ok(bytes)
}

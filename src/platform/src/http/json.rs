use std::collections::BTreeMap;
use std::error::Error;

use axum::async_trait;
use axum::body::HttpBody;
use axum::extract::rejection::JsonRejection;
use axum::extract::FromRequest;
use axum::extract::RequestParts;
use axum::response::IntoResponse;
use axum::BoxError;
use axum_core::response::Response;
use lazy_static::lazy_static;
use regex::Regex;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::ApiError;

#[derive(Debug, Clone, Copy, Default)]
pub struct Json<T>(pub T);

#[async_trait]
impl<T, B> FromRequest<B> for Json<T>
where
    T: DeserializeOwned,
    B: HttpBody + Send,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    type Rejection = ApiError;

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req).await {
            Ok(v) => Ok(Json(v.0)),
            Err(err) => {
                let src_err = err.source().unwrap().source().unwrap();
                let mut api_err = ApiError::bad_request(format!("{err}: {src_err}"));
                // add field information
                match &err {
                    JsonRejection::JsonDataError(_) => {
                        lazy_static! {
                            static ref FIELD_RX: Regex =
                                Regex::new(r"(\w+?) field `(.+?)`").unwrap();
                        }
                        if let Some(captures) = FIELD_RX.captures(src_err.to_string().as_str()) {
                            api_err = api_err.with_fields(BTreeMap::from([(
                                captures[2].to_string(),
                                captures[1].to_string(),
                            )]));
                        }
                    }
                    JsonRejection::JsonSyntaxError(_) => {}
                    JsonRejection::MissingJsonContentType(_) => {}
                    JsonRejection::BytesRejection(_) => {}
                    _ => panic!(),
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

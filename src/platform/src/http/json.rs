use std::future::Future;

use axum::async_trait;
use axum::body::HttpBody;
use axum::extract::rejection::JsonRejection;
use axum::extract::FromRequest;
use axum::extract::RequestParts;
use axum::response::IntoResponse;
use axum::BoxError;
use axum_core::response::Response;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::PlatformError;

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
    type Rejection = PlatformError;

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        match axum::Json::<T>::from_request(req).await {
            Ok(v) => Ok(Json(v.0)),
            Err(err) => Err(PlatformError::BadRequest(format!(
                "json error: {}",
                err.to_string()
            ))),
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

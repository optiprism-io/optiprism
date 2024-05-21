use std::sync::Arc;

use axum::async_trait;
use axum::body::HttpBody;
use axum::extract::Extension;
use axum::extract::FromRequest;
use axum::extract::FromRequestParts;
use axum::http;
use axum::http::request::Parts;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Request;
use axum::middleware::Next;
use axum_core::body;
use axum_core::body::Body;
use axum_core::response::IntoResponse;
use axum_core::response::Response;
use axum_extra::headers::authorization::Bearer;
use axum_extra::headers::Authorization;
use axum_extra::TypedHeader;
use bytes::Bytes;
use common::config::Config;
use serde_json::Value;

use crate::auth::token::parse_access_token;
use crate::error::AuthError;
use crate::PlatformError;
use crate::Result;

#[derive(Default, Clone)]
pub struct Context {
    pub account_id: u64,
    pub organization_id: u64,
}

#[async_trait]
impl<S> FromRequestParts<S> for Context
where S: Send + Sync
{
    type Rejection = PlatformError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> core::result::Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) =
            TypedHeader::<Authorization<Bearer>>::from_request_parts(parts, state)
                .await
                .map_err(|_err| AuthError::CantParseBearerHeader)?;

        let Extension(cfg) = Extension::<Config>::from_request_parts(parts, state)
            .await
            .map_err(|err| PlatformError::Internal(err.to_string()))?;

        let claims = parse_access_token(bearer.token(), "access")
            .map_err(|err| err.wrap_into(AuthError::CantParseAccessToken))?;
        let Extension(md_acc_prov) =
            Extension::<Arc<metadata::accounts::Accounts>>::from_request_parts(parts, state)
                .await
                .map_err(|err| PlatformError::Internal(err.to_string()))?;

        let acc = md_acc_prov.get_by_id(claims.account_id)?;
        let ctx = Context {
            account_id: acc.id,
            organization_id: claims.organization_id,
        };

        Ok(ctx)
    }
}

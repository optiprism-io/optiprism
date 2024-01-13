use std::sync::Arc;

use axum::async_trait;
use axum::body::HttpBody;
use axum::extract::Extension;
use axum::extract::TypedHeader;
use axum::headers::authorization::Bearer;
use axum::headers::Authorization;
use axum::http;
use axum::http::request::Parts;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Request;
use axum::middleware::Next;
use axum_core::body;
use axum_core::extract::FromRequestParts;
use axum_core::response::IntoResponse;
use axum_core::response::Response;
use bytes::Bytes;
use common::rbac::OrganizationPermission;
use common::rbac::OrganizationRole;
use common::rbac::Permission;
use common::rbac::ProjectPermission;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::rbac::ORGANIZATION_PERMISSIONS;
use common::rbac::PERMISSIONS;
use common::rbac::PROJECT_PERMISSIONS;
use hyper::Body;
use serde_json::Value;

use crate::auth;
use crate::auth::token::parse_access_token;
use crate::error::AuthError;
use crate::PlatformError;
use crate::Result;

#[derive(Default, Clone)]
pub struct AuthContext {
    pub account_id: u64,
}

#[derive(Default, Clone)]
pub struct Context {
    pub account_id: Option<u64>,
    pub role: Option<Role>,
    pub organizations: Option<Vec<(u64, OrganizationRole)>>,
    pub projects: Option<Vec<(u64, ProjectRole)>>,
    pub teams: Option<Vec<(u64, Role)>>,
}

impl Context {
    pub fn check_permission(&self, permission: Permission) -> Result<()> {
        if let Some(role) = &self.role {
            for (root_role, role_permission) in PERMISSIONS.iter() {
                if *root_role != *role {
                    continue;
                }
                if role_permission.contains(&Permission::All) {
                    return Ok(());
                }
                if role_permission.contains(&permission) {
                    return Ok(());
                }
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }

    pub fn check_organization_permission(
        &self,
        organization_id: u64,
        permission: OrganizationPermission,
    ) -> Result<()> {
        if self
            .check_permission(Permission::ManageOrganizations)
            .is_ok()
        {
            return Ok(());
        }
        let role = self.get_organization_role(organization_id)?;
        for (org_role, role_permission) in ORGANIZATION_PERMISSIONS.iter() {
            if *org_role != role {
                continue;
            }

            if role_permission.contains(&OrganizationPermission::All) {
                return Ok(());
            }
            if role_permission.contains(&permission) {
                return Ok(());
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }

    pub fn check_project_permission(
        &self,
        organization_id: u64,
        project_id: u64,
        permission: ProjectPermission,
    ) -> Result<()> {
        if self.check_permission(Permission::ManageProjects).is_ok() {
            return Ok(());
        }
        if let Ok(role) = self.get_organization_role(organization_id) {
            match role {
                OrganizationRole::Owner => return Ok(()),
                OrganizationRole::Admin => return Ok(()),
                OrganizationRole::Member => {}
            }
        }

        let role = self.get_project_role(project_id)?;

        for (proj_role, role_permission) in PROJECT_PERMISSIONS.iter() {
            if *proj_role != role {
                continue;
            }

            if role_permission.contains(&ProjectPermission::All) {
                return Ok(());
            }
            if role_permission.contains(&permission) {
                return Ok(());
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }

    fn get_organization_role(&self, organization_id: u64) -> Result<OrganizationRole> {
        if let Some(organizations) = &self.organizations {
            for (org_id, role) in organizations.iter() {
                if *org_id == organization_id {
                    return Ok(role.to_owned());
                }
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }

    fn get_project_role(&self, project_id: u64) -> Result<ProjectRole> {
        if let Some(projects) = &self.projects {
            for (proj_id, role) in projects.iter() {
                if *proj_id == project_id {
                    return Ok(role.to_owned());
                }
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }
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

        let Extension(auth_cfg) = Extension::<auth::Config>::from_request_parts(parts, state)
            .await
            .map_err(|err| PlatformError::Internal(err.to_string()))?;

        let claims = parse_access_token(bearer.token(), &auth_cfg.access_token_key)
            .map_err(|err| err.wrap_into(AuthError::CantParseAccessToken))?;
        let Extension(md_acc_prov) =
            Extension::<Arc<metadata::accounts::Accounts>>::from_request_parts(parts, state)
                .await
                .map_err(|err| PlatformError::Internal(err.to_string()))?;

        let acc = md_acc_prov.get_by_id(claims.account_id)?;
        let ctx = Context {
            account_id: Some(acc.id),
            role: acc.role,
            organizations: acc.organizations,
            projects: acc.projects,
            teams: acc.teams,
        };

        Ok(ctx)
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
            return Err(PlatformError::BadRequest(format!(
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

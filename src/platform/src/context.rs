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
use common::rbac::OrganizationPermission;
use common::rbac::OrganizationRole;
use common::rbac::Permission;
use common::rbac::ProjectPermission;
use common::rbac::ProjectRole;
use common::rbac::Role;
use common::rbac::ORGANIZATION_PERMISSIONS;
use common::rbac::PERMISSIONS;
use common::rbac::PROJECT_PERMISSIONS;
use serde_json::Value;

use crate::auth::token::parse_access_token;
use crate::error::AuthError;
use crate::PlatformError;
use crate::Result;

#[derive(Default, Clone)]
pub struct Context {
    pub account_id: u64,
    pub organization_id: u64,
    pub force_update_password: bool,
    pub role: Option<Role>,
    pub organizations: Option<Vec<(u64, OrganizationRole)>>,
    pub projects: Option<Vec<(u64, ProjectRole)>>,
    pub teams: Option<Vec<(u64, Role)>>,
}

impl Context {
    pub fn check_permission(&self, permission: Permission) -> Result<()> {
        // if self.force_update_password {
        //     return Err(PlatformError::Forbidden(
        //         "password must be changed".to_string(),
        //     ));
        // }
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
        org_id: u64,
        permission: OrganizationPermission,
    ) -> Result<()> {
        if self
            .check_permission(Permission::ManageOrganizations)
            .is_ok()
        {
            return Ok(());
        }
        let role = self.get_organization_role(org_id)?;
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
        org_id: u64,
        project_id: u64,
        permission: ProjectPermission,
    ) -> Result<()> {
        if self.check_permission(Permission::ManageProjects).is_ok() {
            return Ok(());
        }
        if let Ok(role) = self.get_organization_role(org_id) {
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
            force_update_password: acc.force_update_password,
            role: acc.role,
            organizations: acc.organizations,
            projects: acc.projects,
            teams: acc.teams,
        };

        Ok(ctx)
    }
}

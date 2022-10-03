use crate::{accounts, auth, PlatformError};
use crate::Result;
use axum::{
    async_trait,
    extract::{FromRequest, RequestParts, TypedHeader},
    headers::{authorization::Bearer, Authorization},
};

use common::{
    rbac::{Permission, Role},
};
use std::collections::HashMap;
use std::sync::Arc;
use axum::extract::Extension;
use common::rbac::{ORGANIZATION_PERMISSIONS, OrganizationPermission, OrganizationRole, PERMISSIONS, PROJECT_PERMISSIONS, ProjectPermission, ProjectRole};
use crate::auth::auth::parse_access_token;

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
                if root_role == role {
                    if role_permission.contains(&permission) {
                        return Ok(());
                    }
                }
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }

    pub fn check_organization_permission(&self, organization_id: u64, permission: OrganizationPermission) -> Result<()> {
        let role = self.get_organization_role(organization_id)?;
        for (org_role, role_permission) in ORGANIZATION_PERMISSIONS.iter() {
            if *org_role == role {
                if role_permission.contains(&permission) {
                    return Ok(());
                }
            }
        }

        Err(PlatformError::Forbidden("forbidden".to_string()))
    }

    pub fn check_project_permission(&self, organization_id: u64, project_id: u64, permission: ProjectPermission) -> Result<()> {
        match self.get_organization_role(organization_id) {
            Ok(role) => match role {
                OrganizationRole::Owner => return Ok(()),
                OrganizationRole::Admin => return Ok(()),
                OrganizationRole::Member => {}
            }
            Err(_) => {}
        }

        let role = self.get_project_role(project_id)?;

        for (proj_role, role_permission) in PROJECT_PERMISSIONS.iter() {
            if *proj_role == role {
                if role_permission.contains(&permission) {
                    return Ok(());
                }
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
impl<B> FromRequest<B> for Context
    where
        B: Send,
{
    type Rejection = PlatformError;

    async fn from_request(req: &mut RequestParts<B>) -> core::result::Result<Self, Self::Rejection> {
        let TypedHeader(Authorization(bearer)) =
            TypedHeader::<Authorization<Bearer>>::from_request(req)
                .await
                .map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;

        let Extension(auth_prov) = Extension::<Arc<auth::Provider>>::from_request(req)
            .await
            .map_err(|err| PlatformError::Internal(err.to_string()))?;

        let claims = parse_access_token(bearer.token(), &auth_prov.access_token_key).map_err(|err| PlatformError::Unauthorized(format!("{:?}", err)))?;;
        let Extension(md_acc_prov) = Extension::<Arc<metadata::accounts::Provider>>::from_request(req)
            .await
            .map_err(|err| PlatformError::Internal(err.to_string()))?;

        let acc = md_acc_prov.get_by_id(claims.account_id).await.map_err(|err| PlatformError::Internal(err.to_string()))?;
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
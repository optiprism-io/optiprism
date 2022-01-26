use crate::Error;
use crate::Result;
use axum::{
    async_trait,
    extract::{FromRequest, RequestParts, TypedHeader},
    headers::{authorization::Bearer, Authorization},
};
use common::{
    auth::parse_access_token,
    rbac::{Permission, Role, Scope, MANAGER_PERMISSIONS, READER_PERMISSIONS},
};
use std::collections::HashMap;
use common::rbac::{Action, Resource};

#[derive(Default)]
pub struct Context {
    pub organization_id: u64,
    pub project_id: u64,
    pub account_id: u64,
    pub roles: Option<HashMap<Scope, Role>>,
    pub permissions: Option<HashMap<Scope, Vec<Permission>>>,
}

impl Context {
    pub fn with_permission(organization_id: u64, permission: Permission) -> Self {
        let mut ctx = Context::default();
        ctx.organization_id = organization_id;
        let mut permissions = HashMap::new();
        permissions.insert(Scope::Organization, vec![permission]);
        ctx.permissions = Some(permissions);
        ctx
    }

    pub fn check_action_permission(&self, permission: Permission) -> Result<()> {
        Ok(())
    }

    pub fn maybe_check_project_permission(&self, project_id: Option<u64>) -> Result<()> {
        Ok(())
    }

    pub fn check_project_permission(&self, project_id: u64) -> Result<()> {
        Ok(())
    }

    pub fn check_organization_permission(&self, organization_id: u64) -> Result<()> {
        Ok(())
    }

    pub fn check_resource_permission(&self, id: u64, owner_id: u64, resource: Resource, action: Action) -> Result<()> {
        Ok(())
    }

    pub fn get_project_id(&self, other_project_id: Option<u64>) -> u64 {
        match other_project_id {
            None => self.project_id,
            Some(project_id) => project_id
        }
    }

    pub fn is_permitted(
        &self,
        organization_id: u64,
        project_id: u64,
        permission: Permission,
    ) -> bool {
        if organization_id != self.organization_id {
            return false;
        }
        if let Some(roles) = &self.roles {
            for (scope, role) in roles {
                if let Scope::Project(id) = scope {
                    if *id != project_id {
                        continue;
                    }
                }
                match role {
                    Role::Owner => return true,
                    Role::Manager => {
                        if check_permissions(&MANAGER_PERMISSIONS, &permission) {
                            return true;
                        }
                    }
                    Role::Reader => {
                        if check_permissions(&READER_PERMISSIONS, &permission) {
                            return true;
                        }
                    }
                }
            }
        }
        if let Some(permissions) = &self.permissions {
            for (scope, permissions) in permissions {
                if let Scope::Project(id) = scope {
                    if *id != project_id {
                        continue;
                    }
                }
                if check_permissions(permissions, &permission) {
                    return true;
                }
            }
        }
        false
    }
}

fn check_permissions(permissions: &[Permission], permission: &Permission) -> bool {
    for p in permissions {
        if *p == *permission {
            return true;
        }
    }
    false
}

#[async_trait]
impl<B> FromRequest<B> for Context
    where
        B: Send,
{
    type Rejection = Error;

    async fn from_request(request: &mut RequestParts<B>) -> std::Result<Self, Self::Rejection> {
        let mut ctx = Context::default();
        if let Ok(TypedHeader(Authorization(bearer))) =
        TypedHeader::<Authorization<Bearer>>::from_request(request).await
        {
            if let Some(token) = bearer.token().strip_prefix("Bearer ") {
                if let Ok(claims) = parse_access_token(token) {
                    ctx.organization_id = claims.organization_id;
                    ctx.account_id = claims.account_id;
                    ctx.roles = claims.roles;
                    ctx.permissions = claims.permissions;
                }
            }
        }
        Ok(ctx)
    }
}

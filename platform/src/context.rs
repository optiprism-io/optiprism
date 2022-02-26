use crate::Error;
use crate::Result;
use axum::{
    async_trait,
    extract::{FromRequest, RequestParts, TypedHeader},
    headers::{authorization::Bearer, Authorization},
};

use common::{
    auth::parse_access_token,
    rbac::{Permission, Role, Scope},
};
use std::collections::HashMap;

#[derive(Default)]
pub struct Context {
    pub organization_id: u64,
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

    pub fn check_permission(&self, _: u64, _: u64, _: Permission) -> Result<()> {
        return Ok(());
        /*if organization_id != self.organization_id {
            return Err(Error::Internal(InternalError::new("code", StatusCode::FORBIDDEN)));
        }
        if let Some(roles) = &self.roles {
            for (scope, role) in roles {
                if let Scope::Project(id) = scope {
                    if *id != project_id {
                        continue;
                    }
                }
                match role {
                    Role::Owner => return Ok(()),
                    Role::Manager => {
                        if check_permissions(&MANAGER_PERMISSIONS, &permission) {
                            return Ok(());
                        }
                    }
                    Role::Reader => {
                        if check_permissions(&READER_PERMISSIONS, &permission) {
                            return Ok(());
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
                    return Ok(());
                }
            }
        }

        return Err(Error::Internal(InternalError::new("code", StatusCode::FORBIDDEN)));*/
    }
}

/*fn check_permissions(permissions: &[Permission], permission: &Permission) -> bool {
    for p in permissions {
        if *p == *permission {
            return true;
        }
    }
    false
}*/

#[async_trait]
impl<B> FromRequest<B> for Context
where
    B: Send,
{
    type Rejection = Error;

    async fn from_request(
        request: &mut RequestParts<B>,
    ) -> core::result::Result<Self, Self::Rejection> {
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

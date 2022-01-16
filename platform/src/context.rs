use crate::{auth, error::Error};
use actix_http::header;
use actix_utils::future::{err, ok, Ready};
use actix_web::{dev::Payload, FromRequest, HttpRequest};
use common::rbac::{Permission, Role, Scope, MANAGER_PERMISSIONS, READER_PERMISSIONS};
use std::{collections::HashMap, ops::Deref, rc::Rc};

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

    pub fn from_token(token: Option<&header::HeaderValue>) -> Self {
        let mut ctx = Context::default();
        if let Some(value) = token {
            if let Ok(value) = value.to_str() {
                if let Some(token) = value.strip_prefix("Bearer ") {
                    if let Ok(claims) = auth::parse_access_token(token) {
                        ctx.organization_id = claims.organization_id;
                        ctx.account_id = claims.account_id;
                        ctx.roles = claims.roles;
                        ctx.permissions = claims.permissions;
                    }
                }
            }
        }
        ctx
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

pub struct ContextExtractor(Rc<Context>);

impl ContextExtractor {
    pub fn new(state: Context) -> ContextExtractor {
        ContextExtractor(Rc::new(state))
    }

    pub fn into_inner(self) -> Rc<Context> {
        self.0
    }
}

impl Deref for ContextExtractor {
    type Target = Rc<Context>;

    fn deref(&self) -> &Rc<Context> {
        &self.0
    }
}

impl FromRequest for ContextExtractor {
    type Error = Error;
    type Future = Ready<Result<Self, Error>>;

    #[inline]
    fn from_request(request: &HttpRequest, _: &mut Payload) -> Self::Future {
        if let Some(ctx) = request.req_data().get::<ContextExtractor>() {
            ok(ContextExtractor(ctx.0.clone()))
        } else {
            unimplemented!();
            // err(ERR_INTERNAL_CONTEXT_REQUIRED.into())
        }
    }
}

use super::{
    auth,
    error::{Error, ERR_INTERNAL_CONTEXT_REQUIRED},
};
use actix_http::header;
use actix_utils::future::{err, ok, Ready};
use actix_web::{dev::Payload, App, FromRequest, HttpRequest};
use std::{ops::Deref, rc::Rc};

#[derive(Debug, Default)]
pub struct Context {
    organization_id: u64,
    account_id: u64,
}

impl Context {
    pub fn from_token(token: Option<&header::HeaderValue>) -> Self {
        let mut ctx = Context::default();
        if let Some(value) = token {
            if let Ok(value) = value.to_str() {
                if let Some(token) = value.strip_prefix("Bearer ") {
                    if let Ok(claims) = auth::parse_access_token(token) {
                        ctx.organization_id = claims.organization_id;
                        ctx.account_id = claims.account_id;
                    }
                }
            }
        }
        ctx
    }
}

#[derive(Debug)]
pub struct ContextExtractor(Rc<Context>);

impl ContextExtractor {
    pub fn new(state: Context) -> ContextExtractor {
        ContextExtractor(Rc::new(state))
    }

    fn into_inner(self) -> Rc<Context> {
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
    type Config = ();
    type Error = Error;
    type Future = Ready<Result<Self, Error>>;

    #[inline]
    fn from_request(request: &HttpRequest, _: &mut Payload) -> Self::Future {
        if let Some(ctx) = request.extensions().get::<ContextExtractor>() {
            ok(ContextExtractor(ctx.0.clone()))
        } else {
            err(ERR_INTERNAL_CONTEXT_REQUIRED.into())
        }
    }
}

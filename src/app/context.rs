use super::{
    auth,
    error::{Error, ERR_INTERNAL_CONTEXT_REQUIRED},
};
use actix_http::{
    body::{Body, MessageBody},
    header,
};
use actix_service::{
    apply, apply_fn_factory, IntoServiceFactory, ServiceFactory, ServiceFactoryExt, Transform,
};
use actix_utils::future::{err, ok, Ready};
use actix_web::{
    dev::Payload,
    dev::{ServiceRequest, ServiceResponse},
    get,
    web::{Data, ServiceConfig},
    App, FromRequest, HttpRequest, HttpServer,
};
use std::{future::Future, ops::Deref, rc::Rc};

#[derive(Debug, Default)]
struct Context {
    user_id: u64,
}

#[derive(Debug)]
struct ContextExtractor(Rc<Context>);

impl ContextExtractor {
    fn new(state: Context) -> ContextExtractor {
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

pub fn middleware<B, T, R>(request: ServiceRequest, service: &T::Service) -> R
where
    B: MessageBody,
    T: ServiceFactory<
        ServiceRequest,
        Config = (),
        Response = ServiceResponse<B>,
        Error = Error,
        InitError = (),
    >,
    R: Future<Output = Result<ServiceResponse<B>, Error>> + Clone,
{
    let mut ctx = Context::default();
    if let Some(value) = request.headers().get(header::AUTHORIZATION) {
        if let Ok(value) = value.to_str() {
            if let Some(token) = value.strip_prefix("Bearer ") {
                if let Ok(claims) = auth::parse_jwt_access_token(token) {
                    ctx.user_id = claims.user_id;
                }
            }
        }
    }
    request.extensions_mut().insert(ContextExtractor::new(ctx));
    service.call(request)
}

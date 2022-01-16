use crate::{
    auth::{
        provider::Provider,
        types::{LogInRequest, SignUpRequest},
    },
    context::ContextExtractor,
};
use actix_web::{
    post,
    web::{Data, Json, ServiceConfig},
    Error, HttpResponse,
};

#[post("/v1/auth/signup")]
async fn sign_up(
    ctx: ContextExtractor,
    provider: Data<Provider>,
    request: Json<SignUpRequest>,
) -> Result<HttpResponse, Error> {
    let response = provider
        .sign_up(ctx.into_inner(), request.into_inner())
        .await?;
    Ok(HttpResponse::Ok().json(response))
}

#[post("/v1/auth/login")]
async fn log_in(
    ctx: ContextExtractor,
    provider: Data<Provider>,
    request: Json<LogInRequest>,
) -> Result<HttpResponse, Error> {
    let response = provider.log_in(ctx.into_inner(), request.into_inner())?;
    Ok(HttpResponse::Ok().json(response))
}

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.service(sign_up).service(log_in);
}

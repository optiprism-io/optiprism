use super::auth_provider::{LogInRequest, Provider, SignUpRequest};
use actix_web::{post, web, Error, HttpResponse};

#[post("/v1/auth/signup")]
async fn sign_up(
    provider: web::Data<Provider>,
    request: web::Json<SignUpRequest>,
) -> Result<HttpResponse, Error> {
    let response = provider.sign_up(request.into_inner()).await?;
    Ok(HttpResponse::Ok().json(response))
}

#[post("/v1/auth/login")]
async fn log_in(
    provider: web::Data<Provider>,
    request: web::Json<LogInRequest>,
) -> Result<HttpResponse, Error> {
    let response = provider.log_in(request.into_inner())?;
    Ok(HttpResponse::Ok().json(response))
}

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(sign_up).service(log_in);
}

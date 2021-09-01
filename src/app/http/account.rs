use super::{
    account_provider::{Provider, UpdateRequest},
    error::{Result, ERR_TODO},
    ContextExtractor,
};
use actix_web::{
    delete as method_delete, get, patch,
    web::{Data, Json, Path, ServiceConfig},
    HttpResponse,
};

#[get("/v1/accounts")]
async fn list(ctx: ContextExtractor, provider: Data<Provider>) -> Result<HttpResponse> {
    let list = provider.list(ctx.into_inner())?;
    Ok(HttpResponse::Ok().json(list))
}

#[get("/v1/accounts/{id}")]
async fn get_by_id(
    ctx: ContextExtractor,
    provider: Data<Provider>,
    id: Path<u64>,
) -> Result<HttpResponse> {
    let acc = provider.get_by_id(ctx.into_inner(), id.into_inner())?;
    Ok(HttpResponse::Ok().json(acc))
}

#[patch("/v1/accounts/{id}")]
async fn update(
    ctx: ContextExtractor,
    provider: Data<Provider>,
    id: Path<u64>,
    request: Json<UpdateRequest>,
) -> Result<HttpResponse> {
    if id.into_inner() != request.id {
        return Err(ERR_TODO.into());
    }
    let acc = provider.update(ctx.into_inner(), request.into_inner())?;
    Ok(HttpResponse::Ok().json(acc))
}

#[method_delete("/v1/accounts/{id}")]
async fn delete(
    ctx: ContextExtractor,
    provider: Data<Provider>,
    id: Path<u64>,
) -> Result<HttpResponse> {
    let acc = provider.delete(ctx.into_inner(), id.into_inner())?;
    Ok(HttpResponse::Ok().json(acc))
}

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.service(list)
        .service(get_by_id)
        .service(update)
        .service(delete);
}

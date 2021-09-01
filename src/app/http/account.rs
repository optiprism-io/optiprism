use super::{account_provider::Provider, ContextExtractor};
use actix_web::{
    get,
    web::{Data, Path, ServiceConfig},
    Error, HttpResponse,
};

#[get("/v1/accounts/{id}")]
async fn get_by_id(
    ctx: ContextExtractor,
    provider: Data<Provider>,
    id: Path<u64>,
) -> Result<HttpResponse, Error> {
    let acc = provider.get_by_id(ctx.into_inner(), id.into_inner())?;
    Ok(HttpResponse::Ok().json(acc))
}

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.service(get_by_id);
}

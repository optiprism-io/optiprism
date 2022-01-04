use super::organization_provider::Provider;
use actix_web::{
    get,
    web::{Data, Path, ServiceConfig},
    Error, HttpResponse,
};

#[get("/v1/organizations/{id}")]
async fn get_by_id(provider: Data<Provider>, id: Path<u64>) -> Result<HttpResponse, Error> {
    let org = provider.get_by_id(id.into_inner())?;
    Ok(HttpResponse::Ok().json(org))
}

pub fn configure(cfg: &mut ServiceConfig) {
    cfg.service(get_by_id);
}

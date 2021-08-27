mod auth;
mod organization;

use super::auth as auth_provider;
use super::organization as organization_provider;
use actix_web::web::ServiceConfig;

pub fn configure(cfg: &mut ServiceConfig) {
    auth::configure(cfg);
    organization::configure(cfg);
}

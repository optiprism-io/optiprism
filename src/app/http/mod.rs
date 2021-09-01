mod account;
mod auth;
mod organization;

use super::{
    account as account_provider, auth as auth_provider, context::ContextExtractor,
    organization as organization_provider,
};
use actix_web::web::ServiceConfig;

pub fn configure(cfg: &mut ServiceConfig) {
    auth::configure(cfg);
    organization::configure(cfg);
    account::configure(cfg);
}

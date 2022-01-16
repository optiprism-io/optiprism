mod auth;

use actix_web::web::ServiceConfig;

pub fn configure(cfg: &mut ServiceConfig) {
    auth::configure(cfg);
}

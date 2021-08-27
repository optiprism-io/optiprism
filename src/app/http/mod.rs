use super::{FindRequest, ListPublicProfile, Service, UserPublicProfile};
use crate::context::ContextExtractor;
use actix_web::{get, web, HttpResponse};

fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(internal_find_users)
        .service(internal_find_user_by_id)
        .service(find_users)
        .service(find_user_by_id);
}

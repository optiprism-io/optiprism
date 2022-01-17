mod auth;

use axum::Router;

pub fn configure(router: &mut Router) {
    auth::configure(router);
}

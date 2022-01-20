mod auth;

use axum::Router;

pub fn configure(router: Router) -> Router {
    auth::configure(router)
}

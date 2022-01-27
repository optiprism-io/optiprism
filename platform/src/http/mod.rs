mod auth;
mod events;

use axum::Router;

pub fn configure(router: Router) -> Router {
    auth::configure(router)
}

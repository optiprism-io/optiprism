pub mod auth;
pub mod events;
pub mod properties;

use axum::Router;

pub fn configure(router: Router) -> Router {
    auth::configure(router)
}

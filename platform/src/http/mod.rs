pub mod auth;
pub mod event_properties;
pub mod events;

use axum::Router;

pub fn configure(router: Router) -> Router {
    auth::configure(router)
}

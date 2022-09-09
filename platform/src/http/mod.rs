pub mod auth;
pub mod events;
pub mod properties;
pub mod queries;
pub mod custom_events;

use crate::PlatformProvider;
use axum::Router;

pub fn attach_routes(router: Router, platform: PlatformProvider) -> Router {
    let mut router = auth::attach_routes(router, platform.auth.clone());
    router = events::attach_routes(router, platform.events.clone());
    router = custom_events::attach_routes(router, platform.custom_events.clone());
    router = properties::attach_event_routes(router, platform.event_properties.clone());
    router = properties::attach_user_routes(router, platform.user_properties.clone());
    queries::attach_routes(router, platform.query)
}

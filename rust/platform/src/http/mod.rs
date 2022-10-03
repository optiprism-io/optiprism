pub mod accounts;
pub mod auth;
pub mod custom_events;
pub mod events;
pub mod properties;
pub mod queries;

use crate::PlatformProvider;
use axum::Router;
use metadata::Metadata;
use std::sync::Arc;

pub fn attach_routes(mut router: Router, platform: PlatformProvider, md: Arc<Metadata>) -> Router {
    router = accounts::attach_routes(router, platform.accounts.clone(), md.accounts.clone());
    router = auth::attach_routes(router, platform.auth.clone());
    router = events::attach_routes(router, platform.events.clone());
    router = custom_events::attach_routes(router, platform.custom_events.clone());
    router = properties::attach_event_routes(router, platform.event_properties.clone());
    router = properties::attach_user_routes(router, platform.user_properties.clone());
    router = queries::attach_routes(router, platform.query);

    router
}

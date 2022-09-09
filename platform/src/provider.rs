use crate::queries::provider::QueryProvider;
use crate::{AccountsProvider, AuthProvider, CustomEventsProvider, EventsProvider, PropertiesProvider};
use metadata::Metadata;
use std::sync::Arc;

pub struct PlatformProvider {
    pub events: Arc<EventsProvider>,
    pub custom_events: Arc<CustomEventsProvider>,
    pub event_properties: Arc<PropertiesProvider>,
    pub user_properties: Arc<PropertiesProvider>,
    pub accounts: Arc<AccountsProvider>,
    pub auth: Arc<AuthProvider>,
    pub query: Arc<QueryProvider>,
}

impl PlatformProvider {
    pub fn new(md: Arc<Metadata>, query: Arc<QueryProvider>) -> Self {
        Self {
            events: Arc::new(EventsProvider::new(md.events.clone())),
            custom_events: Arc::new(CustomEventsProvider::new(md.custom_events.clone())),
            event_properties: Arc::new(PropertiesProvider::new_event(md.event_properties.clone())),
            user_properties: Arc::new(PropertiesProvider::new_user(md.user_properties.clone())),
            accounts: Arc::new(AccountsProvider::new(md.accounts.clone())),
            auth: Arc::new(AuthProvider::new(md.clone())),
            query,
        }
    }
}

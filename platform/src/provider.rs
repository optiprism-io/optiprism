use crate::{
    AccountsProvider, AuthProvider, EventsProvider, PropertiesProvider,
};
use metadata::Metadata;
use std::sync::Arc;
use crate::queries::provider::QueryProvider;

pub struct PlatformProvider {
    pub events: Arc<EventsProvider>,
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
            event_properties: Arc::new(PropertiesProvider::new_event(md.event_properties.clone())),
            user_properties: Arc::new(PropertiesProvider::new_user(md.user_properties.clone())),
            accounts: Arc::new(AccountsProvider::new(md.accounts.clone())),
            auth: Arc::new(AuthProvider::new(md.clone())),
            query,
        }
    }
}

use crate::queries::provider::QueryProvider;
use crate::{
    AccountsProvider, AuthProvider, CustomEventsProvider, EventsProvider, PropertiesProvider,
};
use chrono::Duration;
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
    pub fn new(
        md: Arc<Metadata>,
        query: Arc<QueryProvider>,
        access_token_duration: Duration,
        access_token_key: String,
        refresh_token_duration: Duration,
        refresh_token_key: String,
    ) -> Self {
        Self {
            events: Arc::new(EventsProvider::new(md.events.clone())),
            custom_events: Arc::new(CustomEventsProvider::new(md.custom_events.clone())),
            event_properties: Arc::new(PropertiesProvider::new_event(md.event_properties.clone())),
            user_properties: Arc::new(PropertiesProvider::new_user(md.user_properties.clone())),
            accounts: Arc::new(AccountsProvider::new(md.accounts.clone())),
            auth: Arc::new(AuthProvider::new(
                md.accounts.clone(),
                access_token_duration,
                access_token_key,
                refresh_token_duration,
                refresh_token_key,
            )),
            query,
        }
    }
}

use crate::{
    AccountsProvider, AuthProvider, EventSegmentationProvider, EventsProvider, PropertiesProvider,
};
use metadata::Metadata;
use query::QueryProvider;
use std::sync::Arc;

pub struct Platform {
    pub events: Arc<EventsProvider>,
    pub event_properties: Arc<PropertiesProvider>,
    pub user_properties: Arc<PropertiesProvider>,
    pub accounts: Arc<AccountsProvider>,
    pub auth: Arc<AuthProvider>,
    pub event_segmentation: Arc<EventSegmentationProvider>,
}

impl Platform {
    pub fn new(md: Arc<Metadata>, query: Arc<QueryProvider>) -> Self {
        Self {
            events: Arc::new(EventsProvider::new(md.events.clone())),
            event_properties: Arc::new(PropertiesProvider::new_event(md.event_properties.clone())),
            user_properties: Arc::new(PropertiesProvider::new_user(md.user_properties.clone())),
            accounts: Arc::new(AccountsProvider::new(md.accounts.clone())),
            auth: Arc::new(AuthProvider::new(md.clone())),
            event_segmentation: Arc::new(EventSegmentationProvider::new(query)),
        }
    }
}

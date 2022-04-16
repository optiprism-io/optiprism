use std::sync::Arc;
use metadata::{database, Metadata, organizations, projects};
use query::QueryProvider;
use crate::{accounts, AccountsProvider, auth, AuthProvider, events, EventsProvider, properties, PropertiesProvider, ReportsProvider};

pub struct Platform {
    pub events: Arc<EventsProvider>,
    pub event_properties: Arc<PropertiesProvider>,
    pub user_properties: Arc<PropertiesProvider>,
    pub accounts: Arc<AccountsProvider>,
    pub auth: Arc<AuthProvider>,
    pub reports: Arc<ReportsProvider>,
}

impl Platform {
    pub fn new(md: Arc<Metadata>, query: Arc<QueryProvider>) -> Self {
        Self {
            events: Arc::new(EventsProvider::new(md.events.clone())),
            event_properties: Arc::new(PropertiesProvider::new_event(md.event_properties.clone())),
            user_properties: Arc::new(PropertiesProvider::new_user(md.user_properties.clone())),
            accounts: Arc::new(AccountsProvider::new(md.accounts.clone())),
            auth: Arc::new(AuthProvider::new(md.clone())),
            reports: Arc::new(ReportsProvider::new(query)),
        }
    }
}
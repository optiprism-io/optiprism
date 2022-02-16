use crate::events::Request;

pub struct Provider {}

impl Provider {
    pub async fn ingest(self, id: String, request: Request) {}
}

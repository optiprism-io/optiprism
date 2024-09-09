use std::collections::HashMap;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use axum::extract::ConnectInfo;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing;
use axum::Extension;
use axum::Router;
use axum_macros::debug_handler;
use chrono::DateTime;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::Deserialize;
use tower_http::cors::Any;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use crate::error::Result;
use crate::executor::Executor;
use crate::RequestContext;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Campaign {
    pub source: String,
    pub medium: Option<String>,
    pub campaign: Option<String>,
    pub term: Option<String>,
    pub content: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Context {
    pub library: Option<Library>,
    pub page: Option<Page>,
    pub user_agent: Option<String>,
    pub ip: Option<IpAddr>,
    pub campaign: Option<Campaign>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Library {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    pub path: Option<String>,
    pub referrer: Option<String>,
    pub search: Option<String>,
    pub title: Option<String>,
    pub url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PropValue {
    Date(DateTime<Utc>),
    String(String),
    Number(Decimal),
    Bool(bool),
}

impl From<PropValue> for crate::PropValue {
    fn from(value: PropValue) -> Self {
        match value {
            PropValue::Date(v) => crate::PropValue::Date(v),
            PropValue::String(v) => crate::PropValue::String(v),
            PropValue::Number(v) => crate::PropValue::Number(v),
            PropValue::Bool(v) => crate::PropValue::Bool(v),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdentifyRequest {
    pub timestamp: Option<DateTime<Utc>>,
    pub context: Context,
    pub group: String,
    pub id: String,
    pub properties: Option<HashMap<String, PropValue>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrackRequest {
    pub user_id: Option<String>,
    pub anonymous_id: Option<String>,
    pub timestamp: Option<DateTime<Utc>>,
    pub context: Context,
    pub event: String,
    pub properties: Option<HashMap<String, PropValue>>,
    pub groups: Option<HashMap<String, String>>,
}

#[debug_handler]
async fn track(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Extension(app): Extension<App>,
    Path(token): Path<String>,
    body: String,
    // common::http::Json(request): common::http::Json<TrackRequest>,
) -> Result<StatusCode> {
    let request: TrackRequest = serde_json::from_str(&body)?;
    let ctx = RequestContext {
        project_id: None,
        client_ip: addr.ip(),
        token,
    };
    app.track(&ctx, request)?;

    Ok(StatusCode::CREATED)
}

#[debug_handler]
async fn identify(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Extension(app): Extension<App>,
    Path(token): Path<String>,
    body: String,
) -> Result<StatusCode> {
    let request: IdentifyRequest = serde_json::from_str(&body)?;
    let ctx = RequestContext {
        project_id: None,
        client_ip: addr.ip(),
        token,
    };
    app.identify(&ctx, request)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Clone)]
struct App {
    track: Arc<Mutex<Executor<crate::Track>>>,
    identify: Arc<Mutex<Executor<crate::Identify>>>,
}

impl App {
    pub fn track(&self, ctx: &RequestContext, req: TrackRequest) -> Result<()> {
        let context = crate::Context {
            library: req.context.library.map(|lib| crate::Library {
                name: lib.name.clone(),
                version: lib.version.clone(),
            }),
            page: req.context.page.map(|page| crate::Page {
                path: page.path.clone(),
                referrer: page.referrer.clone(),
                search: page.search.clone(),
                title: page.title.clone(),
                url: page.url.clone(),
            }),
            user_agent: req.context.user_agent.clone(),
            ip: req.context.ip.unwrap_or_else(|| ctx.client_ip),
            campaign: req.context.campaign.map(|cmp| crate::Campaign {
                source: cmp.source.clone(),
                medium: cmp.medium.clone(),
                campaign: cmp.campaign.clone(),
                term: cmp.term.clone(),
                content: cmp.content.clone(),
            }),
        };

        let raw_properties = req.properties.map(|v| {
            v.into_iter()
                .map(|(k, v)| (k.to_owned(), v.into()))
                .collect::<_>()
        });

        let track = crate::Track {
            user_id: req.user_id.clone(),
            anonymous_id: req.anonymous_id.clone(),
            resolved_user_id: None,
            timestamp: req.timestamp.unwrap_or_else(Utc::now),
            context,
            event: req.event.clone(),
            resolved_event: None,
            properties: raw_properties,
            resolved_properties: None,
            group_values: req.groups.clone(),
            resolved_group_values: None,
        };

        self.track.lock().unwrap().execute(ctx, track)
    }

    pub fn identify(&self, ctx: &RequestContext, req: IdentifyRequest) -> Result<()> {
        let context = crate::Context {
            library: req.context.library.map(|lib| crate::Library {
                name: lib.name.clone(),
                version: lib.version.clone(),
            }),
            page: req.context.page.map(|page| crate::Page {
                path: page.path.clone(),
                referrer: page.referrer.clone(),
                search: page.search.clone(),
                title: page.title.clone(),
                url: page.url.clone(),
            }),
            user_agent: req.context.user_agent.clone(),
            ip: req.context.ip.unwrap_or_else(|| ctx.client_ip),
            campaign: req.context.campaign.map(|cmp| crate::Campaign {
                // todo do we need to add utm to user profiles?
                campaign: cmp.campaign.clone(),
                source: cmp.source.clone(),
                medium: cmp.medium.clone(),
                term: cmp.term.clone(),
                content: cmp.content.clone(),
            }),
        };

        let raw_props = req.properties.map(|v| {
            v.into_iter()
                .map(|(k, v)| (k.to_owned(), v.into()))
                .collect::<_>()
        });
        let track = crate::Identify {
            timestamp: req.timestamp.unwrap_or_else(Utc::now),
            context,
            group: req.group,
            group_id: 0,
            resolved_group_values: None,
            id: req.id,
            properties: raw_props,
            resolved_properties: None,
        };

        self.identify.lock().unwrap().execute(ctx, track)
    }
}

pub fn attach_routes(
    router: Router,
    track_exec: Executor<crate::Track>,
    identify_exec: Executor<crate::Identify>,
) -> Router {
    let app = App {
        track: Arc::new(Mutex::new(track_exec)),
        identify: Arc::new(Mutex::new(identify_exec)),
    };
    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_origin(Any)
        .allow_headers(Any);

    router
        .route("/api/v1/ingest/:token/track", routing::post(track))
        .route("/api/v1/ingest/:token/identify", routing::post(identify))
        .layer(cors)
        .layer(Extension(app))
        .layer(Extension(TraceLayer::new_for_http()))
}

#[cfg(test)]
mod tests {
    use crate::sources::http::TrackRequest;

    #[test]
    fn test_request() {
        const PAYLOAD: &str = r#"
        {
  "anonymousId": "23adfd82-aa0f-45a7-a756-24f2a7a4c895",
  "context": {
    "library": {
      "name": "analytics.js",
      "version": "2.11.1"
    },
    "page": {
      "path": "/academy/",
      "referrer": "",
      "search": "",
      "title": "Analytics Academy",
      "url": "https://segment.com/academy/"
    },
    "userAgent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.86 Safari/537.36",
    "ip": "108.0.78.21"
  },
  "event": "Course Clicked",
  "integrations": {},
  "messageId": "ajs-f8ca1e4de5024d9430b3928bd8ac6b96",
  "properties": {
    "title": "Intro to Analytics"
  },
  "receivedAt": "2015-12-12T19:11:01.266Z",
  "sentAt": "2015-12-12T19:11:01.169Z",
  "timestamp": "2015-12-12T19:11:01.249Z",
  "type": "track",
  "userId": "AiUGstSDIg",
  "originalTimestamp": "2015-12-12T19:11:01.152Z"
}
    "#;

        let _: TrackRequest = serde_json::from_str(PAYLOAD).unwrap();
    }
}

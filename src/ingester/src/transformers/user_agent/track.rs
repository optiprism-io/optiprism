use std::sync::Arc;

use metadata::properties::Properties;
use uaparser::UserAgentParser;

use crate::error::Result;
use crate::transformers::user_agent::resolve_properties;
use crate::RequestContext;
use crate::Track;
use crate::Transformer;

pub struct UserAgent {
    ua_parser: UserAgentParser,
    properties: Arc<Properties>,
}

impl UserAgent {
    pub fn try_new(properties: Arc<Properties>, ua_parser: UserAgentParser) -> Result<Self> {
        Ok(Self {
            ua_parser,
            properties,
        })
    }
}

impl Transformer<Track> for UserAgent {
    fn process(&self, ctx: &RequestContext, mut req: Track) -> Result<Track> {
        if req.context.user_agent.is_none() {
            return Ok(req);
        }
        let mut props = if let Some(props) = &req.resolved_properties {
            props.to_owned()
        } else {
            vec![]
        };
        props = resolve_properties(ctx, &req.context, props, &self.properties, &self.ua_parser)?;
        if !props.is_empty() {
            req.resolved_properties = Some(props);
        }

        Ok(req)
    }
}

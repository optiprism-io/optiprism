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
    user_properties: Arc<Properties>,
}

impl UserAgent {
    pub fn try_new(user_properties: Arc<Properties>, ua_parser: UserAgentParser) -> Result<Self> {
        Ok(Self {
            ua_parser,
            user_properties,
        })
    }
}

impl Transformer<Track> for UserAgent {
    fn process(&self, ctx: &RequestContext, mut req: Track) -> Result<Track> {
        if req.context.user_agent.is_none() {
            return Ok(req);
        }
        let mut user_props = if let Some(props) = &req.resolved_user_properties {
            props.to_owned()
        } else {
            vec![]
        };
        user_props = resolve_properties(
            ctx,
            &req.context,
            user_props,
            &self.user_properties,
            &self.ua_parser,
        )?;
        if !user_props.is_empty() {
            req.resolved_user_properties = Some(user_props);
        }

        Ok(req)
    }
}

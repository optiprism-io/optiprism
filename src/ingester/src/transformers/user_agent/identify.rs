use std::fs;
use std::io;
use std::sync::Arc;

use common::types;
use futures::executor::block_on;
use metadata::events;
use metadata::properties;
use uaparser::Parser;
use uaparser::UserAgentParser;

use crate::error::IngesterError;
use crate::error::Result;
use crate::transformers::user_agent::resolve_properties;
use crate::Identify;
use crate::PropValue;
use crate::PropertyAndValue;
use crate::RequestContext;
use crate::Track;
use crate::Transformer;

pub struct UserAgent {
    ua_parser: UserAgentParser,
    user_properties: Arc<dyn properties::Provider>,
}

impl UserAgent {
    pub fn try_new(
        user_properties: Arc<dyn properties::Provider>,
        ua_parser: UserAgentParser,
    ) -> Result<Self> {
        Ok(Self {
            ua_parser,
            user_properties,
        })
    }
}

impl Transformer<Identify> for UserAgent {
    fn process(&self, ctx: &RequestContext, mut req: Identify) -> Result<Identify> {
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

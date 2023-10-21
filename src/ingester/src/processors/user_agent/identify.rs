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
use crate::processors::user_agent::resolve_properties;
use crate::Identify;
use crate::Processor;
use crate::PropValue;
use crate::Property;
use crate::RequestContext;
use crate::Track;

pub struct UserAgent {
    ua_parser: UserAgentParser,
    user_properties: Arc<dyn properties::Provider>,
}

impl UserAgent {
    pub fn try_new(
        user_properties: Arc<dyn properties::Provider>,
        db_path: fs::File,
    ) -> Result<Self> {
        let ua_parser = UserAgentParser::from_file(db_path)
            .map_err(|e| IngesterError::General(e.to_string()))?;

        Ok(Self {
            ua_parser,
            user_properties,
        })
    }
}

impl Processor<Identify> for UserAgent {
    fn process(&self, ctx: &RequestContext, mut req: Identify) -> Result<Identify> {
        let context = req.context.clone();
        if context.user_agent.is_none() {
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

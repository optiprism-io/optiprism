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
use crate::processor::Processor;
use crate::track::PropValue;
use crate::track::Property;
use crate::track::Track;
use crate::AppContext;

pub struct UserAgent {
    ua_parser: UserAgentParser,
    event_properties: Arc<dyn properties::Provider>,
    user_properties: Arc<dyn properties::Provider>,
    events: Arc<dyn events::Provider>,
}

impl UserAgent {
    pub fn try_new(
        event_properties: Arc<dyn properties::Provider>,
        user_properties: Arc<dyn properties::Provider>,
        events: Arc<dyn events::Provider>,
        db_path: fs::File,
    ) -> Result<Self> {
        let ua_parser = UserAgentParser::from_file(db_path)
            .map_err(|e| IngesterError::General(e.to_string()))?;

        Ok(Self {
            ua_parser,
            event_properties,
            user_properties,
            events,
        })
    }
}

impl Processor for UserAgent {
    fn track(&self, ctx: &AppContext, mut track: Track) -> crate::error::Result<Track> {
        let context = track.context.clone();
        if context.user_agent.is_none() {
            return Ok(track);
        }
        let ua = context.user_agent.clone().unwrap();
        let client = self.ua_parser.parse(&ua);
        let mut user_props = if let Some(props) = &track.resolved_properties {
            track.resolved_properties.to_owned().unwrap()
        } else {
            vec![]
        };

        // client family
        {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_CLIENT_FAMILY,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(client.user_agent.family.to_string()),
            };
            user_props.push(prop);
        }

        // client version major
        if let Some(v) = client.user_agent.major {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_CLIENT_VERSION_MAJOR,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(v.to_string()),
            };
            user_props.push(prop);
        }

        // client version minor
        if let Some(v) = client.user_agent.minor {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_CLIENT_VERSION_MINOR,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(v.to_string()),
            };
            user_props.push(prop);
        }

        // device family
        {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_DEVICE_FAMILY,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(client.device.family.to_string()),
            };

            user_props.push(prop);
        }

        // device brand
        if let Some(brand) = client.device.brand {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_DEVICE_BRAND,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(brand.to_string()),
            };

            user_props.push(prop);
        }

        // device brand
        if let Some(model) = client.device.model {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_DEVICE_MODEL,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(model.to_string()),
            };

            user_props.push(prop);
        }

        // os family
        {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_OS_FAMILY,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(client.os.family.to_string()),
            };

            user_props.push(prop);
        }

        // os major
        if let Some(v) = client.os.major {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_OS_VERSION_MAJOR,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(v.to_string()),
            };

            user_props.push(prop);
        }

        // os minor
        if let Some(v) = client.os.minor {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_OS_VERSION_MINOR,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(v.to_string()),
            };

            user_props.push(prop);
        }

        // os major
        if let Some(v) = client.os.patch {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_OS_VERSION_PATCH,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(v.to_string()),
            };

            user_props.push(prop);
        }

        // os major
        if let Some(v) = client.os.patch_minor {
            let prop = block_on(self.user_properties.get_by_name(
                ctx.organization_id,
                ctx.project_id,
                types::USER_COLUMN_OS_VERSION_PATCH_MINOR,
            ))?;

            let prop = Property {
                id: prop.id,
                value: PropValue::String(v.to_string()),
            };

            user_props.push(prop);
        }

        track.resolved_properties = Some(user_props);

        Ok(track)
    }
}
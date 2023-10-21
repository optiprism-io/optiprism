use std::sync::Arc;

use common::types;
use futures::executor::block_on;
use metadata::properties;
use uaparser::Parser;
use uaparser::UserAgentParser;

use crate::Context;
use crate::PropValue;
use crate::Property;
use crate::RequestContext;
use crate::Track;

pub mod identify;
pub mod track;

use crate::error::Result;

pub fn resolve_properties(
    ctx: &RequestContext,
    context: &Context,
    mut user_props: Vec<Property>,
    props_prov: &Arc<dyn properties::Provider>,
    ua_parser: &UserAgentParser,
) -> Result<Vec<Property>> {
    let ua = context.user_agent.clone().unwrap();
    let client = ua_parser.parse(&ua);

    // client family
    {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_CLIENT_FAMILY,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_CLIENT_FAMILY.to_string(),
            value: PropValue::String(client.user_agent.family.to_string()),
        };
        user_props.push(prop);
    }

    // client version major
    if let Some(v) = client.user_agent.major {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_CLIENT_VERSION_MAJOR,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_CLIENT_VERSION_MAJOR.to_string(),
            value: PropValue::String(v.to_string()),
        };
        user_props.push(prop);
    }

    // client version minor
    if let Some(v) = client.user_agent.minor {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_CLIENT_VERSION_MINOR,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_CLIENT_VERSION_MINOR.to_string(),
            value: PropValue::String(v.to_string()),
        };
        user_props.push(prop);
    }

    // device family
    {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_DEVICE_FAMILY,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_DEVICE_FAMILY.to_string(),
            value: PropValue::String(client.device.family.to_string()),
        };

        user_props.push(prop);
    }

    // device brand
    if let Some(brand) = client.device.brand {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_DEVICE_BRAND,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_DEVICE_BRAND.to_string(),
            value: PropValue::String(brand.to_string()),
        };

        user_props.push(prop);
    }

    // device brand
    if let Some(model) = client.device.model {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_DEVICE_MODEL,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_DEVICE_MODEL.to_string(),
            value: PropValue::String(model.to_string()),
        };

        user_props.push(prop);
    }

    // os family
    {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_OS_FAMILY,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_OS_FAMILY.to_string(),
            value: PropValue::String(client.os.family.to_string()),
        };

        user_props.push(prop);
    }

    // os major
    if let Some(v) = client.os.major {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_OS_VERSION_MAJOR,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_OS_VERSION_MAJOR.to_string(),
            value: PropValue::String(v.to_string()),
        };

        user_props.push(prop);
    }

    // os minor
    if let Some(v) = client.os.minor {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_OS_VERSION_MINOR,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_OS_VERSION_MINOR.to_string(),
            value: PropValue::String(v.to_string()),
        };

        user_props.push(prop);
    }

    // os major
    if let Some(v) = client.os.patch {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_OS_VERSION_PATCH,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_OS_VERSION_PATCH.to_string(),
            value: PropValue::String(v.to_string()),
        };

        user_props.push(prop);
    }

    // os major
    if let Some(v) = client.os.patch_minor {
        let prop = block_on(props_prov.get_by_name(
            ctx.organization_id,
            ctx.project_id,
            types::USER_COLUMN_OS_VERSION_PATCH_MINOR,
        ))?;

        let prop = Property {
            id: prop.id,
            name: types::USER_COLUMN_OS_VERSION_PATCH_MINOR.to_string(),
            value: PropValue::String(v.to_string()),
        };

        user_props.push(prop);
    }

    Ok(user_props)
}

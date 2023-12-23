use std::sync::Arc;

use common::types;
use futures::executor::block_on;
use metadata::properties;
use uaparser::Parser;
use uaparser::UserAgentParser;

use crate::Context;
use crate::PropValue;
use crate::PropertyAndValue;
use crate::RequestContext;
use crate::Track;

pub mod identify;
pub mod track;

use crate::error::Result;

pub fn resolve_properties(
    ctx: &RequestContext,
    context: &Context,
    mut user_props: Vec<PropertyAndValue>,
    props_prov: &Arc<dyn properties::Provider>,
    ua_parser: &UserAgentParser,
) -> Result<Vec<PropertyAndValue>> {
    let ua = context.user_agent.clone().unwrap();
    let client = ua_parser.parse(&ua);

    let org_id = ctx.organization_id.unwrap();
    let proj_id = ctx.project_id.unwrap();
    // client family
    {
        let property =
            props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_CLIENT_FAMILY)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(client.user_agent.family.to_string()),
        };
        user_props.push(prop);
    }

    // client version major
    if let Some(v) = client.user_agent.major {
        let property =
            props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_CLIENT_VERSION_MAJOR)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(v.to_string()),
        };
        user_props.push(prop);
    }

    // client version minor
    if let Some(v) = client.user_agent.minor {
        let property =
            props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_CLIENT_VERSION_MINOR)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(v.to_string()),
        };
        user_props.push(prop);
    }

    // device family
    {
        let property =
            props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_DEVICE_FAMILY)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(client.device.family.to_string()),
        };

        user_props.push(prop);
    }

    // device brand
    if let Some(brand) = client.device.brand {
        let property =
            props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_DEVICE_BRAND)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(brand.to_string()),
        };

        user_props.push(prop);
    }

    // device brand
    if let Some(model) = client.device.model {
        let property =
            props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_DEVICE_MODEL)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(model.to_string()),
        };

        user_props.push(prop);
    }

    // os
    {
        let property = props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_OS)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(client.os.family.to_string()),
        };

        user_props.push(prop);
    }

    // os family
    {
        let property = props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_OS_FAMILY)?;

        let mut s = String::new();
        s.push_str(client.os.family.as_ref());
        if let Some(major) = &client.os.major {
            s.push_str(" ");
            s.push_str(major.to_string().as_ref());
            if let Some(minor) =&client.os.minor {
                s.push_str(".");
                s.push_str(minor.to_string().as_ref());
                if let Some(patch) = &client.os.patch {
                    s.push_str(".");
                    s.push_str(patch.to_string().as_ref());
                    if let Some(pm) = &client.os.patch_minor {
                        s.push_str(".");
                        s.push_str(pm.to_string().as_ref());
                    }
                }
            }
        }
        let prop = PropertyAndValue {
            property,
            value: PropValue::String(s),
        };

        user_props.push(prop);
    }

    // os major
    if let Some(v) = client.os.major {
        let property =
            props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_OS_VERSION_MAJOR)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(v.to_string()),
        };

        user_props.push(prop);
    }

    // os minor
    if let Some(v) = client.os.minor {
        let property =
            props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_OS_VERSION_MINOR)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(v.to_string()),
        };

        user_props.push(prop);
    }

    // os major
    if let Some(v) = client.os.patch {
        let property =
            props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_OS_VERSION_PATCH)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(v.to_string()),
        };

        user_props.push(prop);
    }

    // os major
    if let Some(v) = client.os.patch_minor {
        let property =
            props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_OS_VERSION_PATCH_MINOR)?;

        let prop = PropertyAndValue {
            property,
            value: PropValue::String(v.to_string()),
        };

        user_props.push(prop);
    }

    Ok(user_props)
}

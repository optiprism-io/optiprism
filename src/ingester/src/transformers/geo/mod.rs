pub mod identify;
pub mod track;

use std::sync::Arc;

use common::types;
use futures::executor::block_on;
use maxminddb;
use maxminddb::geoip2;
use metadata::properties;

use crate::error::Result;
use crate::Context;
use crate::PropValue;
use crate::Property;
use crate::RequestContext;

pub fn resolve_properties(
    ctx: &RequestContext,
    context: &Context,
    mut user_props: Vec<Property>,
    props_prov: &Arc<dyn properties::Provider>,
    city_rdr: &maxminddb::Reader<Vec<u8>>,
) -> Result<Vec<Property>> {
    let org_id = ctx.organization_id.unwrap();
    let proj_id = ctx.project_id.unwrap();

    let ip = match context.ip {
        Some(ip) => ip,
        None => ctx.client_ip,
    };
    let city: geoip2::City = city_rdr.lookup(ip)?;
    if let Some(country) = city.country {
        if let Some(names) = country.names {
            if let Some(name) = names.get("en") {
                let prop = props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_COUNTRY)?;

                let prop = Property {
                    property: prop,
                    value: PropValue::String(name.to_string()),
                };
                user_props.push(prop);
            }
        }
    }

    if let Some(city) = city.city {
        if let Some(names) = city.names {
            if let Some(name) = names.get("en") {
                let prop = props_prov.get_by_name(org_id, proj_id, types::USER_PROPERTY_CITY)?;

                let prop = Property {
                    property: prop,
                    value: PropValue::String(name.to_string()),
                };
                user_props.push(prop);
            }
        }
    }

    Ok(user_props)
}

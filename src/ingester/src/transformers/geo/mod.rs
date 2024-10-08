pub mod track;
use std::sync::Arc;

use common::types;
use maxminddb;
use maxminddb::geoip2;
use metadata::properties::Properties;

use crate::error::Result;
use crate::Context;
use crate::PropValue;
use crate::PropertyAndValue;
use crate::RequestContext;

pub fn resolve_properties(
    ctx: &RequestContext,
    context: &Context,
    mut props: Vec<PropertyAndValue>,
    props_prov: &Arc<Properties>,
    city_rdr: &maxminddb::Reader<Vec<u8>>,
) -> Result<Vec<PropertyAndValue>> {
    let proj_id = ctx.project_id.unwrap();

    let city: geoip2::City = city_rdr.lookup(context.ip)?;
    if let Some(country) = city.country {
        if let Some(names) = country.names {
            if let Some(name) = names.get("en") {
                let prop = props_prov.get_by_name(proj_id, types::EVENT_PROPERTY_COUNTRY)?;

                let prop = PropertyAndValue {
                    property: prop,
                    value: PropValue::String(name.to_string()),
                };
                props.push(prop);
            }
        }
    }

    if let Some(city) = city.city {
        if let Some(names) = city.names {
            if let Some(name) = names.get("en") {
                let prop = props_prov.get_by_name(proj_id, types::EVENT_PROPERTY_CITY)?;

                let prop = PropertyAndValue {
                    property: prop,
                    value: PropValue::String(name.to_string()),
                };
                props.push(prop);
            }
        }
    }

    Ok(props)
}

use std::sync::Arc;

use maxminddb::MaxMindDBError;
use metadata::properties::Properties;

use crate::error::IngesterError;
use crate::error::Result;
use crate::transformers::geo::resolve_properties;
use crate::RequestContext;
use crate::Track;
use crate::Transformer;

pub struct Geo {
    city_rdr: maxminddb::Reader<Vec<u8>>,
    properties: Arc<Properties>,
}

impl Geo {
    pub fn try_new(
        properties: Arc<Properties>,
        city_rdr: maxminddb::Reader<Vec<u8>>,
    ) -> Result<Self> {
        Ok(Self {
            city_rdr,
            properties,
        })
    }
}

impl Transformer<Track> for Geo {
    fn process(&self, ctx: &RequestContext, mut req: Track) -> Result<Track> {
        let props = if let Some(props) = &req.resolved_properties {
            props.to_owned()
        } else {
            vec![]
        };

        match resolve_properties(ctx, &req.context, props, &self.properties, &self.city_rdr) {
            Ok(p) => {
                if !p.is_empty() {
                    req.resolved_properties = Some(p);
                }
            }
            Err(e) => match e {
                IngesterError::Maxmind(e) => match e {
                    MaxMindDBError::AddressNotFoundError(_) => {}
                    other => return Err(other.into()),
                },
                other => return Err(other),
            },
        };

        Ok(req)
    }
}

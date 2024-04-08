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
    user_properties: Arc<Properties>,
}

impl Geo {
    pub fn try_new(
        user_properties: Arc<Properties>,
        city_rdr: maxminddb::Reader<Vec<u8>>,
    ) -> Result<Self> {
        Ok(Self {
            city_rdr,
            user_properties,
        })
    }
}

impl Transformer<Track> for Geo {
    fn process(&self, ctx: &RequestContext, mut req: Track) -> Result<Track> {
        let mut user_props = if let Some(props) = &req.resolved_user_properties {
            props.to_owned()
        } else {
            vec![]
        };

        match resolve_properties(
            ctx,
            &req.context,
            user_props,
            &self.user_properties,
            &self.city_rdr,
        ) {
            Ok(user_props) => {
                if !user_props.is_empty() {
                    req.resolved_user_properties = Some(user_props);
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

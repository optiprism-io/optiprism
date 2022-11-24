use arrow::array::ArrayRef;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::{array_ref_to_json_values, ListResponse, ResponseMetadata};
use crate::json_value_to_scalar;
use crate::EventRef;
use crate::PlatformError;
use crate::PropValueOperation;
use crate::PropertyRef;
use crate::Result;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Filter {
    pub operation: PropValueOperation,
    pub value: Option<Vec<Value>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListPropertyValuesRequest {
    #[serde(flatten)]
    pub property: PropertyRef,
    #[serde(flatten)]
    pub event: Option<EventRef>,
    pub filter: Option<Filter>,
}

impl TryInto<query::queries::property_values::PropertyValues> for ListPropertyValuesRequest {
    type Error = PlatformError;

    fn try_into(
        self,
    ) -> std::result::Result<query::queries::property_values::PropertyValues, Self::Error> {
        Ok(query::queries::property_values::PropertyValues {
            property: self.property.try_into()?,
            event: self.event.map(|event| event.into()),
            filter: self.filter.map(|filter| filter.try_into()).transpose()?,
        })
    }
}

impl TryInto<query::queries::property_values::Filter> for Filter {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<query::queries::property_values::Filter, Self::Error> {
        Ok(query::queries::property_values::Filter {
            operation: self.operation.try_into()?,
            value: self
                .value
                .map(|values| {
                    values
                        .iter()
                        .map(json_value_to_scalar)
                        .collect::<Result<_>>()
                })
                .transpose()?,
        })
    }
}

impl TryInto<ListResponse<Value>> for ArrayRef {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<ListResponse<Value>, Self::Error> {
        Ok(ListResponse {
            data: array_ref_to_json_values(&self)?,
            meta: ResponseMetadata { next: None },
        })
    }
}

use crate::queries::types::{json_value_to_scalar, EventRef, PropValueOperation, PropertyRef};
use crate::{array_ref_to_json_values, PlatformError, Result};
use arrow::array::ArrayRef;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Filter {
    pub operation: PropValueOperation,
    pub value: Option<Vec<Value>>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PropertyValues {
    pub property: PropertyRef,
    pub event: Option<EventRef>,
    pub filter: Option<Filter>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResponse {
    values: Vec<Value>,
}

impl TryInto<query::queries::property_values::PropertyValues> for PropertyValues {
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

impl TryInto<ListResponse> for ArrayRef {
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<ListResponse, Self::Error> {
        Ok(ListResponse {
            values: array_ref_to_json_values(&self)?,
        })
    }
}

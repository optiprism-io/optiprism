use crate::Result;
use datafusion::scalar::ScalarValue as DFScalarValue;

pub enum ID {
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
}

impl ID {
    pub fn to_df_scalar_value(self) -> DFScalarValue {
        match self {
            ID::UInt8(v) => DFScalarValue::from(v),
            ID::UInt16(v) => DFScalarValue::from(v),
            ID::UInt32(v) => DFScalarValue::from(v),
            ID::UInt64(v) => DFScalarValue::from(v),
        }
    }
}

pub trait Provider {
    async fn get_id_by_key(&self, table: &str, key: &str) -> Result<ID>;
    async fn get_key_by_id(&self, table: &str, id: ID) -> Result<String>;
}

pub struct ProviderImpl {}

impl Provider for ProviderImpl {
    async fn get_id_by_key(&self, table: &str, key: &str) -> Result<ID> {
        todo!()
    }

    async fn get_key_by_id(&self, table: &str, value: ID) -> Result<String> {
        todo!()
    }
}
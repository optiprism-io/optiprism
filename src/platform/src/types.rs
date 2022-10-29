use serde::Deserialize;
use serde::Serialize;

use crate::PlatformError;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResponseMetadata {
    pub next: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResponse<T> {
    pub data: Vec<T>,
    pub meta: ResponseMetadata,
}

impl<A, B> TryInto<ListResponse<A>> for metadata::metadata::ListResponse<B>
where
    B: TryInto<A> + Clone,
    PlatformError: std::convert::From<<B as TryInto<A>>::Error>,
{
    type Error = PlatformError;

    fn try_into(self) -> std::result::Result<ListResponse<A>, Self::Error> {
        let data = self
            .data
            .into_iter()
            .map(|v| v.try_into())
            .collect::<std::result::Result<Vec<A>, B::Error>>()?;
        let meta = ResponseMetadata {
            next: self.meta.next,
        };
        Ok(ListResponse { data, meta })
    }
}

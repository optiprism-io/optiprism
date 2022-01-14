use crate::{account, event};

pub struct Metadata {
    pub event: event::Provider,
    pub account: account::Provider,
}

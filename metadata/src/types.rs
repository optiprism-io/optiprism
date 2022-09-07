use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum EventRef {
    Regular(String),
    Custom(u64),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum PropValueOperation {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    True,
    False,
    Exists,
    Empty,
    ArrAll,
    ArrAny,
    ArrNone,
    Like,
    NotLike,
    Regex,
    NotRegex,
}


#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum PropertyRef {
    User(String),
    Event(String),
    Custom(u64),
}

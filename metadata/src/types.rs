
#[derive(Clone, Debug)]
pub enum EventRef {
    Regular(String),
    Custom(u64),
}

#[derive(Clone, Debug)]
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


#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum PropertyRef {
    User(String),
    Event(String),
    Custom(u64),
}

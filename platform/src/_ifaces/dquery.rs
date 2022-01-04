use datafusion::scalar::ScalarValue;

enum Condition {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

enum Property {
    User(Condition, ScalarValue),
    Event(Condition, ScalarValue),
}

struct Event {
    name: String,
    properties: Vec<Property>,
}

pub struct EventSegmentation {
    events: Vec<Event>,
}

struct Funnel {}

struct Retention {}

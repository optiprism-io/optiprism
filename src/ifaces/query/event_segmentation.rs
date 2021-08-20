use datafusion::scalar::ScalarValue;

enum Operator {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

enum Aggregate {
    Count(distinct),

}

enum Property {
    User {
        name: String,
        op: Operator,
        value: ScalarValue,
    },
    Event {
        name: String,
        op: Operator,
        value: ScalarValue,
    },
}

enum GroupBy {
    User(String), // string - имя свойства
    Event(String),
}

struct Event {
    name: String,
    properties: Vec<Property>,
    group_by: Vec<Property>,
    aggregate_by: Vec<Aggregate>,
}

struct EventSegmentation {
    events: Vec<Event>,
}
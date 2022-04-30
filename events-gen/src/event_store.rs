use std::collections::HashMap;

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
enum EventProperty {}

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
enum UserProperty {}

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
enum Value {}

#[derive(Debug, Clone)]
pub struct EventRecord {
    event: Event,
    event_properties: HashMap<EventProperty, Value>,
    user_properties: HashMap<UserProperty, Value>,
}

impl EventRecord {
    pub fn new(e: Event) -> EventRecord {
        EventRecord {
            event: e,
            event_properties: Default::default(),
            user_properties: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EventStore {
    events: Vec<EventRecord>,
    // fixed props
    event_properties: HashMap<EventProperty, Value>,
    user_properties: HashMap<UserProperty, Value>,
}

impl EventStore {
    pub fn push(&mut self, e: EventRecord) {
        let merged_event = EventRecord {
            event: e.event.clone(),
            event_properties: {
                let mut ep = self.event_properties.clone();
                ep.extend(e.event_properties.clone());
                ep
            },
            user_properties: {
                let mut up = self.user_properties.clone();
                up.extend(e.user_properties.clone());
                up
            },
        };

        println!("{:?}", e);
        self.events.push(merged_event)
    }
}
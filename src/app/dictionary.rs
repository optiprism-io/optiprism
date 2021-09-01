use parking_lot::RwLock;
use std::collections::HashMap;

pub struct Dictionary {
    values: Vec<String>,
    index: HashMap<String, usize>,
    guard: RwLock<()>,
}

impl Dictionary {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            index: HashMap::new(),
            guard: RwLock::new(()),
        }
    }

    pub fn get_by_id(&self, id: usize) -> Option<&str> {
        let _guard = self.guard.read();
        if id > self.values.len() {
            return None;
        }
        Some(&self.values[id - 1])
    }

    pub fn set(&mut self, value: &str) -> usize {
        {
            let _guard = self.guard.read();
            if let Some(value) = self.index.get(value) {
                return *value;
            }
        }
        let _guard = self.guard.write();
        if let Some(value) = self.index.get(value) {
            return *value;
        }
        self.values.push(value.to_string());
        let id = self.values.len();
        self.index.insert(value.to_string(), id);
        id
    }
}

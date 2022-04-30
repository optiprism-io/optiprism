use std::collections::HashMap;

pub struct State {
    pub products_bought: HashMap<u64, usize>,
    pub products_viewed: HashMap<u64, usize>,
}

impl State {
    pub fn new()->Self {
        Self {
            products_bought: Default::default(),
            products_viewed: Default::default()
        }
    }
}
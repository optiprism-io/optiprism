use criterion::{black_box, criterion_group, criterion_main, Criterion};

use parking_lot::RwLock;
use std::{
    collections::HashMap,
    sync::atomic::{AtomicPtr, Ordering},
};

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

    pub fn get(&self, id: usize) -> Option<&str> {
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

pub struct DictionaryAtomic {
    values: AtomicPtr<Vec<String>>,
    index: HashMap<String, usize>,
    guard: RwLock<()>,
    log: Vec<String>,
    next_index: usize,
}

impl DictionaryAtomic {
    pub fn new() -> Self {
        Self {
            values: AtomicPtr::new(Box::into_raw(Box::new(Vec::new()))),
            index: HashMap::new(),
            guard: RwLock::new(()),
            log: Vec::new(),
            next_index: 0,
        }
    }

    pub fn get(&self, id: usize) -> Option<&str> {
        let values = unsafe { &*self.values.load(Ordering::Relaxed) };
        let len = values.len();
        let index = id - 1;
        if index < len {
            return Some(&values[index]);
        }
        let _guard = self.guard.read();
        if index < self.next_index {
            return Some(&self.log[index - len]);
        }
        None
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
        self.log.push(value.to_string());
        self.next_index += 1;
        let id = self.next_index;
        self.index.insert(value.to_string(), id);
        id
    }

    pub fn merge(&mut self) {
        let mut values = unsafe { &*self.values.load(Ordering::Relaxed) }.clone();
        let _guard = self.guard.write();
        values.append(&mut self.log);
        self.values
            .store(Box::into_raw(Box::new(values)), Ordering::Relaxed);
    }
}

fn dictionary_bench(c: &mut Criterion) {
    let foo = "foo";
    let bar = "bar";

    let mut raw_dict = {
        let mut dict = Dictionary::new();
        assert_eq!(dict.get(black_box(1)), None);
        assert_eq!(dict.get(black_box(2)), None);

        dict.set(foo);
        assert_eq!(dict.get(black_box(1)), Some(foo));
        assert_eq!(dict.get(black_box(2)), None);

        assert_eq!(dict.get(black_box(1)), Some(foo));
        assert_eq!(dict.get(black_box(2)), None);

        dict.set(bar);
        assert_eq!(dict.get(black_box(1)), Some(foo));
        assert_eq!(dict.get(black_box(2)), Some(bar));

        dict
    };

    let mut atomic_dict = {
        let mut dict = DictionaryAtomic::new();
        assert_eq!(dict.get(black_box(1)), None);
        assert_eq!(dict.get(black_box(2)), None);

        dict.set(foo);
        assert_eq!(dict.get(black_box(1)), Some(foo));
        assert_eq!(dict.get(black_box(2)), None);

        dict.merge();
        assert_eq!(dict.get(black_box(1)), Some(foo));
        assert_eq!(dict.get(black_box(2)), None);

        dict.set(bar);
        assert_eq!(dict.get(black_box(1)), Some(foo));
        assert_eq!(dict.get(black_box(2)), Some(bar));

        dict
    };

    c.bench_function("raw/get", |b| b.iter(|| raw_dict.get(black_box(1))));
    c.bench_function("raw/get2", |b| b.iter(|| raw_dict.get(black_box(2))));
    c.bench_function("raw/set", |b| b.iter(|| raw_dict.set(black_box(foo))));

    c.bench_function("atomic/get", |b| b.iter(|| atomic_dict.get(black_box(1))));
    c.bench_function("atomic/get2", |b| b.iter(|| atomic_dict.get(black_box(2))));
    c.bench_function("atomic/set", |b| b.iter(|| atomic_dict.set(black_box(foo))));
}

criterion_group!(benches, dictionary_bench);
criterion_main!(benches);

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use parking_lot::RwLock;
use std::{
    collections::HashMap,
    str::FromStr,
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

    pub fn get(&self, id: usize) -> Option<&String> {
        let _guard = self.guard.read();
        if id > self.values.len() {
            return None;
        }
        Some(&self.values[id - 1])
    }

    pub fn set(&mut self, value: &String) -> usize {
        {
            let _guard = self.guard.read();
            if let Some(value) = self.index.get(value) {
                return value.clone();
            }
        }
        let _guard = self.guard.write();
        if let Some(value) = self.index.get(value) {
            return value.clone();
        }
        self.values.push(value.clone());
        let id = self.values.len();
        self.index.insert(value.clone(), id);
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

    pub fn get(&self, id: usize) -> Option<&String> {
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

    pub fn set(&mut self, value: &String) -> usize {
        {
            let _guard = self.guard.read();
            if let Some(value) = self.index.get(value) {
                return value.clone();
            }
        }
        let _guard = self.guard.write();
        if let Some(value) = self.index.get(value) {
            return value.clone();
        }
        self.log.push(value.clone());
        self.next_index += 1;
        let id = self.next_index;
        self.index.insert(value.clone(), id);
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
    let foo = &String::from("foo");
    let foo_tiny = &TinyStrAuto::from_str(foo).unwrap();
    let bar = &String::from("bar");
    let bar_tiny = &TinyStrAuto::from_str(bar).unwrap();

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

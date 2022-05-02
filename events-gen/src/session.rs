use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;
use chrono::{DateTime, Duration, Utc};
use rand::rngs::ThreadRng;
use rand::distributions::{WeightedIndex};
use rand::prelude::*;
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct EventRecord {
    event_name: String,
    created_at: DateTime<Utc>,
    properties: Option<Vec<Property>>,
}

impl EventRecord {
    pub fn new(event_name: String, created_at: DateTime<Utc>, properties: Option<Vec<Property>>) -> Self {
        Self {
            event_name,
            created_at,
            properties,
        }
    }
}

#[derive(Debug, Clone)]
enum Value {
    Number(Option<f64>),
    String(Option<String>),
    Boolean(Option<bool>),
    DateTime(Option<DateTime<Utc>>),
}

#[derive(Debug, Clone)]
pub struct Property {
    name: String,
    value: Value,
}

pub trait OutputWriter {
    fn write(&mut self, event: &EventRecord, user_props: &Option<Vec<Property>>) -> Result<()>;
}

#[derive(Debug, Clone)]
pub enum Probability {
    Uniform(f64),
    Steps(Vec<f64>),
}

#[derive(Debug, Clone)]
pub struct TransitionRules {
    probability: Probability,
    limit: Option<usize>,
}

impl TransitionRules {
    pub fn new(probability: Probability, limit: Option<usize>) -> Self {
        Self {
            probability,
            limit,
        }
    }
}

pub struct TransitionState {
    probability: Probability,
    limit: Option<usize>,
    pub evaluations_count: usize,
}

impl TransitionState {
    pub fn new(probability: Probability, limit: Option<usize>) -> Self {
        Self {
            probability,
            limit,
            evaluations_count: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ActionTransitionState {
    pub evaluations_count: usize,
}

impl ActionTransitionState {
    pub fn new() -> Self {
        Self {
            evaluations_count: 0
        }
    }
}

pub trait Action<T> {
    fn evaluate(&self, session: &mut Session<T>) -> Result<()>;
    fn name(&self) -> String;
}

pub struct Session<T> {
    pub cur_time: DateTime<Utc>,
    pub rng: ThreadRng,
    pub data: T,
    pub transitions: HashMap<(String, String), TransitionState>,
    pub output: Box<dyn OutputWriter>,
    pub event_properties: Option<Vec<Property>>,
    pub user_properties: Option<Vec<Property>>,
    action_keys: Vec<String>,
    actions: HashMap<String, Rc<dyn Action<T>>>,
}

impl<T> Session<T> {
    pub fn new<A: ToString>(
        cur_time: DateTime<Utc>,
        data: T,
        output: Box<dyn OutputWriter>,
        actions: Vec<Rc<dyn Action<T>>>,
        transitions: Vec<((A, A), TransitionRules)>,
        user_properties: Option<Vec<Property>>,
    ) -> Self {
        Self {
            rng: Default::default(),
            data,
            output,
            event_properties: None,
            user_properties,
            cur_time,
            transitions: transitions.iter().map(|t| ((t.0.0.to_string(), t.0.1.to_string()), TransitionState::new(t.1.probability.clone(), t.1.limit.clone()))).collect(),
            action_keys: actions.iter().map(|action| action.name()).collect(),
            actions: actions.into_iter().map(|action| (action.name(), action)).collect(),
        }
    }

    pub fn wait_between(&mut self, from: Duration, to: Duration) {
        let wait = Duration::seconds(self.rng.gen_range(from.num_seconds()..=to.num_seconds()));
        self.cur_time = self.cur_time.clone() + wait;
    }

    pub fn check_transition_probability<A: ToString>(&mut self, from: A, to: A) -> bool {
        let state = match self.transitions.get_mut(&(from.to_string(), to.to_string())) {
            None => return false,
            Some(v) => v
        };

        if state.limit.is_some() && state.evaluations_count > state.limit.unwrap() {
            return false;
        }

        match &state.probability {
            Probability::Uniform(prob) => self.rng.gen::<f64>() < *prob,
            Probability::Steps(steps) => {
                if state.evaluations_count > steps.len() - 1 {
                    false
                } else {
                    self.rng.gen::<f64>() < steps[state.evaluations_count]
                }
            }
        }
    }

    pub fn run<A: ToString>(&mut self, action: A) -> Result<()> {
        self.actions.get(&action.to_string()).unwrap().clone().evaluate(self)?;
        Ok(())
    }

    pub fn transit<A: ToString>(&mut self, from: A, to: A) -> Result<()> {
        let state =
            self.transitions
                .get_mut(&(from.to_string(), to.to_string())).ok_or_else(|| Error::Internal("transition not found".to_owned()))?;
        state.evaluations_count += 1;

        self.actions.get(&to.to_string()).unwrap().clone().evaluate(self)?;
        Ok(())
    }

    pub fn try_transit<A: ToString + Clone>(&mut self, from: A, to: A) -> Result<bool> {
        if !self.check_transition_probability(from.clone(), to.clone()) {
            return Ok(false);
        }

        self.transit(from, to)?;
        Ok(true)
    }

    pub fn try_auto_transit<A: ToString + Clone>(&mut self, from: A) -> Result<bool> {
        let from_str = from.to_string();
        let keys = self.action_keys.clone();
        for to in keys {
            if from_str != *to {
                if self.check_transition_probability(from_str.clone(), to.clone()) {
                    self.transit(from_str, to.to_string())?;
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    pub fn write_event<N: ToString>(&mut self, name: N, props: Option<Vec<Property>>) -> Result<()> {
        let event = EventRecord::new(name.to_string(), self.cur_time.clone(), props);
        self.output.write(&event, &self.user_properties)?;
        Ok(())
    }
}
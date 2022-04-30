use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;
use chrono::{DateTime, Duration, Utc};
use rand::rngs::ThreadRng;
use rand::distributions::{WeightedIndex};
use rand::prelude::*;
use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct EventRecord<EP> {
    event_name: String,
    created_at: DateTime<Utc>,
    properties: Option<EP>,
}

impl<EP> EventRecord<EP> {
    pub fn new(event_name: String, created_at: DateTime<Utc>, properties: Option<EP>) -> Self {
        Self {
            event_name,
            created_at,
            properties,
        }
    }
}

pub trait OutputWriter<EP, UP> {
    fn write(&mut self, event: &EventRecord<EP>, user_props: &UP) -> Result<()>;
}

#[derive(Debug, Clone)]
pub enum Probability {
    Uniform(f64),
    Steps(Vec<f64>),
}

#[derive(Debug, Clone)]
pub struct TransitionRule {
    probability: Probability,
    limit: Option<usize>,
}

impl TransitionRule {
    pub fn new(probability: Probability, limit: Option<usize>) -> Self {
        Self {
            probability,
            limit,
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

pub trait Action<DP, DS, I, EP, UP, A> where EP: Clone, UP: Clone, A: Clone + Hash + Eq + PartialEq {
    fn evaluate(&self, session: &mut Session<DP, DS, I, EP, UP, A>) -> Result<()>;
    // fn name() -> A where Self: Sized;
    fn dyn_name(&self) -> A;
}

pub struct Session<DP, DS, I, EP, UP, A> where EP: Clone, UP: Clone, A: Clone + Hash + Eq + PartialEq {
    pub cur_time: DateTime<Utc>,
    pub rng: ThreadRng,
    pub data_provider: DP,
    pub data_state: DS,
    pub input: I,
    pub transition_probabilities: HashMap<ActionTransition<A>, TransitionRule>,
    pub transitions_state: HashMap<ActionTransition<A>, ActionTransitionState>,
    pub output: Box<dyn OutputWriter<EP, UP>>,
    pub event_properties: EP,
    pub user_properties: UP,
    actions: HashMap<A, Rc<dyn Action<DP, DS, I, EP, UP, A>>>,
    action_keys: Vec<A>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ActionTransition<A> where A: Clone + Hash + Eq + PartialEq {
    from: A,
    to: A,
}

impl<A> ActionTransition<A> where A: Clone + Hash + Eq + PartialEq {
    pub fn new(from: A, to: A) -> Self {
        Self {
            from,
            to,
        }
    }
}

impl<DP, DS, I, EP, UP, A> Session<DP, DS, I, EP, UP, A> where EP: Clone, UP: Clone, A: Clone + Hash + Eq + PartialEq {
    pub fn new(
        cur_time: DateTime<Utc>,
        data_provider: DP,
        data_state: DS,
        input: I,
        output: Box<dyn OutputWriter<EP, UP>>,
        actions: Vec<Rc<dyn Action<DP, DS, I, EP, UP, A>>>,
        transition_probabilities: Vec<(ActionTransition<A>, TransitionRule)>,
        ep: EP,
        up: UP,
    ) -> Self {
        Self {
            rng: Default::default(),
            data_provider,
            data_state,
            input,
            output,
            event_properties: ep,
            cur_time,
            transition_probabilities: transition_probabilities.into_iter().collect(),
            transitions_state: Default::default(),
            action_keys: actions.iter().map(|action| action.dyn_name()).collect(),
            actions: actions.into_iter().map(|action| (action.dyn_name(), action)).collect(),
            user_properties: up,
        }
    }

    pub fn wait_between(&mut self, from: Duration, to: Duration) {
        let wait = Duration::seconds(self.rng.gen_range(from.num_seconds()..=to.num_seconds()));
        self.cur_time = self.cur_time.clone() + wait;
    }

    pub fn check_transition_probability(&mut self, transition: &ActionTransition<A>) -> bool {
        let rule = match self.transition_probabilities.get(&transition) {
            None => return false,
            Some(v) => v
        };

        match self.transitions_state.get(&transition) {
            Some(state) => {
                if rule.limit.is_some() && state.evaluations_count > rule.limit.unwrap() {
                    return false;
                }

                match &rule.probability {
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
            None => match &rule.probability {
                Probability::Uniform(prob) => self.rng.gen::<f64>() < *prob,
                Probability::Steps(steps) => {
                    self.rng.gen::<f64>() < steps[0]
                }
            },
        }
    }

    pub fn run(&mut self, action: A) -> Result<()> {
        self.actions.get(&action).unwrap().clone().evaluate(self)?;
        Ok(())
    }

    pub fn transit(&mut self, action_transition: ActionTransition<A>) -> Result<()> {
        let state =
            self.transitions_state
                .entry(action_transition.clone())
                .or_insert(ActionTransitionState::new());
        state.evaluations_count += 1;

        self.actions.get(&action_transition.to).unwrap().clone().evaluate(self)?;
        Ok(())
    }

    pub fn try_transit(&mut self, action_transition: ActionTransition<A>) -> Result<bool> {
        if !self.check_transition_probability(&action_transition) {
            return Ok(false);
        }

        self.transit(action_transition)?;
        Ok(true)
    }

    pub fn try_auto_transit(&mut self, from: A) -> Result<bool> where A: Clone {
        let keys = self.action_keys.clone();
        for to in keys.iter() {
            if from != *to {
                let transition = ActionTransition::new(from.clone(), to.clone());
                if self.check_transition_probability(&transition) {
                    self.transit(transition)?;
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    pub fn write_event(&mut self, name: String, props: Option<EP>) -> Result<()> {
        let event = EventRecord::new(name.clone(), self.cur_time.clone(), props);
        self.output.write(&event, &self.user_properties)?;
        Ok(())
    }
}
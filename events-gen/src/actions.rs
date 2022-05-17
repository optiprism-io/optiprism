use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use rand::rngs::ThreadRng;
use crate::error::{Result};
use rand::prelude::*;

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
/*
#[derive(Debug, Clone)]
pub struct TransitionState {
    pub closed: bool,
    pub probs: Vec<f64>,
    pub evaluations_count: usize,
}

impl TransitionState {
    pub fn new(probs: Vec<f64>) -> Self {
        Self {
            closed: false,
            probs,
            evaluations_count: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransitionsTo<T> {
    prob_sum: f64,
    to: Vec<(T, TransitionState)>,
}

impl<T: Clone + Eq + PartialEq + Hash + Debug> TransitionsTo<T> {
    pub fn new(to: Vec<(T, Vec<f64>)>) -> Self {
        Self {
            prob_sum: to.iter().map(|(_, probs)| probs[0]).sum(),
            to: to.iter().map(|(to, probs)| (to.clone(), TransitionState::new(probs.clone()))).collect(),
        }
    }
    pub fn sample(&mut self, rng: &mut ThreadRng, coefficient: Option<(T, f64)>) -> Option<T> {
        let mut remaining_distance = rng.gen::<f64>() * self.prob_sum;
        /*let mut remaining_distance: f64 = match &coefficient {
            None => rng.gen::<f64>() * self.prob_sum,
            Some(c) => rng.gen::<f64>() * (self.prob_sum + c.1)
        };*/

        for (to, state) in self.to.iter_mut() {
            if state.closed {
                continue;
            }

            /*let prob = if let Some((c_to, c)) = &coefficient {
                if c_to == to {
                    println!("CCC");

                    state.probs[state.evaluations_count] * c
                } else {
                    state.probs[state.evaluations_count]
                }
            } else {
                state.probs[state.evaluations_count]
            };

            remaining_distance -= prob;
*/
            remaining_distance -= state.probs[state.evaluations_count];
            if remaining_distance < 0. {
                self.prob_sum -= state.probs[state.evaluations_count];
                state.evaluations_count += 1;
                if state.evaluations_count > state.probs.len() - 1 {
                    state.closed = true;
                } else {
                    self.prob_sum += state.probs[state.evaluations_count];
                }

                return Some(to.clone());
            }
        }

        None
    }
}
*/
/*
pub struct Actions<T: Clone + Eq + PartialEq + Hash> {
    rng: ThreadRng,
    transitions: HashMap<T, TransitionsTo<T>>,
}

impl<T: Clone + Eq + PartialEq + Hash + Copy + Debug> Actions<T> {
    pub fn new(rng: ThreadRng, transitions: HashMap<T, Vec<(T, Vec<f64>)>>) -> Self {
        Self {
            rng,
            transitions: transitions
                .iter()
                .map(|(from, to)| (from.clone(), TransitionsTo::new(to.clone()))).collect(),
        }
    }

    pub fn next_action(&mut self, from: T, coefficient: Option<(T, f64)>) -> Option<T> {
        match self.transitions.get_mut(&from) {
            None => unreachable!(),
            Some(v) => { v.sample(&mut self.rng, coefficient) }
        }
    }

    /*pub fn next_action(&mut self, from: T, coefficient: Option<(T, f64)>) -> Option<T> {
        let keys = self.keys.clone();
        for to in &keys {
            if from == *to {
                continue;
            }

            let coeff = coefficient.and_then(|(target, c)| if *to == target { Some(c) } else { None });
            /*                let coeff = match &coefficient {
                                None => None,
                                Some((target, c)) => if to == target { Some(*c) } else { None }
                            };
            */
            let prob = self.transition_probability(from.clone(), to.clone(), coeff);
            if self.check_probability(prob) {
                return Some(*to);
            }
        }

        None
    }*/

    /*pub fn check_probability(&mut self, prob: f64) -> bool {
        self.rng.gen::<f64>() < prob
    }

    pub fn transition_probability(&mut self, from: T, to: T, coefficient: Option<f64>) -> f64 {
        let state = match self.transitions.get_mut(&(from, to)) {
            None => return 0.,
            Some(v) => v
        };

        if state.limit.is_some() && state.evaluations_count > state.limit.unwrap() {
            return 0.;
        }

        let coefficient = coefficient.unwrap_or(1.);

        match &state.probability {
            Probability::Uniform(prob) => *prob * coefficient,
            Probability::Steps(steps) => {
                if state.evaluations_count > steps.len() - 1 {
                    0.
                } else {
                    steps[state.evaluations_count] * coefficient
                }
            }
        }
    }*/
}*/
use crate::error::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Duration, Utc};

use rand::rngs::ThreadRng;
use std::collections::HashMap;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;

use events_gen::generator::Generator;
use std::thread;

use common::DECIMAL_SCALE;
use crossbeam_channel::tick;
use log::info;

use rand::prelude::*;

use crate::store::actions::Action;
use crate::store::batch_builder::RecordBatchBuilder;
use crate::store::coefficients::make_coefficients;
use crate::store::events::Event;
use crate::store::intention::{select_intention, Intention};
use crate::store::products::{Product, ProductProvider};
use crate::store::transitions::make_transitions;
use rust_decimal::Decimal;

pub struct State<'a> {
    pub session_id: usize,
    pub event_id: usize,
    pub user_id: u64,
    pub cur_timestamp: i64,
    pub selected_product: Option<&'a Product>,
    pub products_bought: HashMap<usize, usize>,
    pub products_viewed: HashMap<usize, usize>,
    pub products_refunded: HashMap<usize, ()>,
    pub cart: Vec<&'a Product>,
    pub search_query: Option<String>,
    pub spent_total: Decimal,
}

pub struct Config {
    pub rng: ThreadRng,
    pub gen: Generator,
    pub schema: SchemaRef,
    pub events_map: HashMap<Event, u64>,
    pub products: ProductProvider,
    pub to: DateTime<Utc>,
    pub batch_size: usize,
    pub partitions: usize,
}

pub struct Scenario {
    pub rng: ThreadRng,
    pub gen: Generator,
    pub schema: SchemaRef,
    pub events_map: HashMap<Event, u64>,
    pub products: ProductProvider,
    pub to: DateTime<Utc>,
    pub batch_size: usize,
    pub partitions: usize,
}

impl Scenario {
    pub fn new(cfg: Config) -> Self {
        Self {
            rng: cfg.rng,
            gen: cfg.gen,
            schema: cfg.schema,
            events_map: cfg.events_map,
            products: cfg.products,
            to: cfg.to,
            batch_size: cfg.batch_size,
            partitions: cfg.partitions,
        }
    }

    pub async fn run(&mut self) -> Result<Vec<Vec<RecordBatch>>> {
        let mut result: Vec<Vec<RecordBatch>> =
            vec![Vec::with_capacity(self.batch_size); self.partitions];
        let mut batch_builders: Vec<RecordBatchBuilder> = Vec::with_capacity(self.partitions);
        for _ in 0..self.partitions {
            batch_builders.push(RecordBatchBuilder::new(
                self.batch_size,
                self.schema.clone(),
            ));
        }

        let events_per_sec = Arc::new(AtomicUsize::new(0));
        let events_per_sec_clone = events_per_sec.clone();
        let users_per_sec = Arc::new(AtomicUsize::new(0));
        let users_per_sec_clone = users_per_sec.clone();
        let to_timestamp = self.to.timestamp();
        let is_ended = Arc::new(AtomicBool::new(false));
        let is_ended_cloned = is_ended.clone();

        thread::spawn(move || {
            let ticker = tick(StdDuration::from_secs(1));
            for _ in ticker {
                if is_ended_cloned.load(Ordering::SeqCst) {
                    break;
                }
                let _ups = users_per_sec_clone.swap(0, Ordering::SeqCst);
                let _eps = events_per_sec_clone.swap(0, Ordering::SeqCst);
                // println!("users per second: {ups}");
                // println!("events per second: {eps}");
            }
        });

        let mut user_id: u64 = 0;
        let mut overall_events: usize = 0;
        let mut partition_id: usize;
        while let Some(sample) = self.gen.next_sample() {
            users_per_sec.fetch_add(1, Ordering::SeqCst);
            user_id += 1;
            partition_id = user_id as usize % self.partitions;
            let batch_builder = &mut batch_builders[partition_id];
            let partition_result = &mut result[partition_id];

            let mut state = State {
                session_id: 0,
                event_id: 0,
                user_id,
                cur_timestamp: sample.cur_timestamp,
                selected_product: None,
                products_bought: Default::default(),
                products_viewed: Default::default(),
                products_refunded: Default::default(),
                cart: vec![],
                search_query: None,
                spent_total: Decimal::new(0, DECIMAL_SCALE as u32),
            };

            'session: loop {
                state.search_query = None;
                state.selected_product = None;

                let products = &self.products;
                let rng = &mut self.rng;
                let intention = select_intention(&state, products, rng);
                if state.session_id > 0 {
                    let add_time = match intention {
                        Intention::BuyCertainProduct(_) => Duration::weeks(2).num_seconds(),
                        Intention::BuyAnyProduct => Duration::weeks(2).num_seconds(),
                        Intention::JustBrowse => Duration::weeks(2).num_seconds(),
                        Intention::MakeRefund(_) => Duration::weeks(1).num_seconds(),
                    };

                    state.cur_timestamp +=
                        self.rng.gen_range(add_time..=add_time + add_time / 10) as i64;
                }
                let mut coefficients = make_coefficients(&intention);
                if self.rng.gen::<f64>() < coefficients.global_bounce_rate {
                    break 'session;
                }

                let mut transitions = make_transitions(&coefficients);
                let mut prev_action: Option<Action> = None;
                let mut action = Action::ViewIndex;
                let mut wait_time: u64;

                'events: loop {
                    events_per_sec.fetch_add(1, Ordering::SeqCst);
                    match (prev_action, action, intention) {
                        (
                            _,
                            Action::EndSession,
                            Intention::JustBrowse
                            | Intention::BuyAnyProduct
                            | Intention::BuyCertainProduct(_),
                        ) => {
                            if state.cart.is_empty() {
                                break 'events;
                            }

                            // coefficients.view_product_to_buy = 1.;
                            // transitions = make_transitions(&coefficients);
                            action = Action::ViewCart;
                        }
                        (_, Action::EndSession, _) => break 'events,
                        (
                            Some(Action::SearchProduct),
                            Action::ViewProduct,
                            Intention::BuyCertainProduct(product),
                        ) => {
                            state.search_query =
                                Some(self.products.string_name(product.name).await?);
                            state.selected_product = Some(product);
                        }
                        (Some(Action::SearchProduct), Action::ViewProduct, _) => {
                            for (idx, product) in self.products.products.iter().enumerate() {
                                if state.products_viewed.contains_key(&product.id) {
                                    continue;
                                }
                                if self.rng.gen::<f64>() < self.products.product_weights[idx] {
                                    state.selected_product = Some(product);
                                    state.search_query =
                                        Some(self.products.string_name(product.name).await?);
                                    break;
                                }
                            }

                            if state.selected_product.is_none() {
                                (prev_action, action) = (Some(action), Action::EndSession);
                                continue;
                            }
                        }
                        (Some(Action::ViewIndexPromotions), Action::ViewProduct, _) => {
                            let sp = self.products.promoted_product_sample(&mut self.rng);
                            if state.products_viewed.contains_key(&sp.id) {
                                action = Action::EndSession;
                                continue;
                            }
                            let _ = state.selected_product.insert(sp);
                        }
                        (_, Action::AddProductToCart, _) => {
                            state.cart.push(state.selected_product.unwrap());
                        }
                        (_, Action::CompleteOrder, _) => {
                            for product in state.cart.iter() {
                                *state.products_bought.entry(product.id).or_insert(0) += 1;
                                state.spent_total += product.final_price();
                            }
                        }
                        (Some(Action::ViewDeals), Action::ViewProduct, _) => {
                            let sp = self.products.deal_product_sample(&mut self.rng);
                            if state.products_viewed.contains_key(&sp.id) {
                                action = Action::EndSession;
                                continue;
                            }
                            let _ = state.selected_product.insert(sp);
                            *state
                                .products_viewed
                                .entry(state.selected_product.unwrap().id)
                                .or_insert(0) += 1;
                        }
                        (_, Action::ViewRelatedProduct, _) => {
                            let product = &state.selected_product.unwrap();
                            let found = self
                                .products
                                .products
                                .iter()
                                .find(|p| p.category == product.category && p.id != product.id);

                            if found.is_none() {
                                // println!("no related product");
                                action = Action::EndSession;
                                continue;
                            }
                            state.selected_product = found;
                            *state
                                .products_viewed
                                .entry(state.selected_product.unwrap().id)
                                .or_insert(0) += 1;
                        }
                        (
                            Some(Action::ViewOrders),
                            Action::RefundProduct,
                            Intention::MakeRefund(product),
                        ) => {
                            state.spent_total -= product.final_price();
                        }
                        (_, Action::Bounce, _) => {
                            break 'events;
                        }
                        (_, Action::AbandonCart, _) => {
                            break 'events;
                        }
                        _ => {}
                    }

                    prev_action = Some(action);
                    (action, wait_time) = next_action(action, &transitions, &mut self.rng);

                    if let Some(event) = prev_action.unwrap().to_event() {
                        overall_events += 1;
                        batch_builder.write_event(
                            event,
                            *self.events_map.get(&event).unwrap(),
                            &state,
                            &sample.profile,
                        )?;
                        if batch_builder.len() >= self.batch_size {
                            partition_result.push(batch_builder.build_record_batch()?);
                        }
                    }

                    #[allow(clippy::single_match)]
                    match prev_action {
                        Some(Action::CompleteOrder) => {
                            state.cart.clear();
                        }
                        _ => {}
                    }
                    state.cur_timestamp +=
                        self.rng.gen_range(wait_time..=wait_time + wait_time / 10) as i64;
                    if state.cur_timestamp > to_timestamp {
                        break 'session;
                    }

                    if state.event_id == 0 {
                        coefficients.bounce_rate = 0.;
                        transitions = make_transitions(&coefficients);
                    }
                    state.event_id += 1;
                }

                state.session_id += 1;
            }
        }

        is_ended.store(true, Ordering::Relaxed);

        info!("total events: {overall_events}");

        // flush the rest
        for (idx, builder) in batch_builders.iter_mut().enumerate() {
            if builder.len() > 0 {
                result[idx].push(builder.build_record_batch()?);
            }
        }

        // remove unused partitions
        result = result.iter().cloned().filter(|v| !v.is_empty()).collect();

        Ok(result)
    }
}

/// from, to, probability, wait time in secs
pub type Transition = (Action, Vec<(Action, f64, u64)>);

pub fn next_action(from: Action, transitions: &[Transition], rng: &mut ThreadRng) -> (Action, u64) {
    for (t_from, to) in transitions.iter() {
        if *t_from != from {
            continue;
        }

        let mut total_weights: f64 = 0.;
        for (_, weight, _) in to.iter() {
            total_weights += weight;
        }

        total_weights *= rng.gen::<f64>();
        for (t_to, weight, wtime) in to.iter() {
            total_weights -= weight;
            if total_weights < 0. {
                return (*t_to, *wtime);
            }
        }

        unreachable!("{total_weights} weight>0")
    }

    unreachable!("transition from {} does not exist", from);
}

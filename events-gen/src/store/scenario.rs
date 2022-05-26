use std::pin::Pin;
use std::collections::HashMap;
use std::{fmt, io, thread};
use std::fmt::{Display, Formatter};
use std::time::Duration as StdDuration;
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use crate::error::{Result};
use std::string::ToString;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Instant;
use arrow::array::{ArrayBuilder, ArrayRef, DecimalArray, DecimalBuilder, Int8Array, Int8Builder, TimestampSecondArray, TimestampSecondBuilder, UInt16Builder, UInt64Builder, UInt8Builder};
use arrow::datatypes::{DataType, DECIMAL_MAX_PRECISION, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use rand::distributions::WeightedIndex;
use rand::rngs::ThreadRng;
// use crate::actions::{Actions, Probability, TransitionState};
use crate::probability;
use crate::store::products::{Dict, Preferences, Product, ProductProvider};
use rand::prelude::*;
use crate::generator::Generator;
use crossbeam_channel::tick;
use common::{DECIMAL_PRECISION, DECIMAL_SCALE};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use metadata::database::TableType;
use metadata::{events, Metadata};
use crate::store::actions::Action;
use crate::store::batch_builder::RecordBatchBuilder;
use crate::store::coefficients::make_coefficients;
use crate::store::events::Event;
use crate::store::intention::{Intention, select_intention};
use crate::store::transitions::make_transitions;

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
    pub search_query: Option<&'a str>,
    pub spent_total: Decimal,
}

pub fn run(
    mut rng: ThreadRng,
    gen: &mut Generator,
    schema: SchemaRef,
    events_map: HashMap<Event, u16>,
    products: &mut ProductProvider,
    to: DateTime<Utc>,
    batch_size: usize,
    partitions: usize,
) -> Result<Vec<Vec<RecordBatch>>> {
    let mut result: Vec<Vec<RecordBatch>> = vec![Vec::with_capacity(batch_size); partitions];
    let mut batch_builder = RecordBatchBuilder::new(batch_size, schema);
    let events_per_sec = Arc::new(AtomicUsize::new(0));
    let events_per_sec_clone = events_per_sec.clone();
    let users_per_sec = Arc::new(AtomicUsize::new(0));
    let users_per_sec_clone = users_per_sec.clone();
    let to_timestamp = to.timestamp();
    thread::spawn(move || {
        let ticker = tick(StdDuration::from_secs(1));

        for _ in ticker {
            let ups = users_per_sec_clone.swap(0, Ordering::SeqCst);
            let eps = events_per_sec_clone.swap(0, Ordering::SeqCst);
            println!("users per second: {ups}");
            println!("events per second: {eps}");
        }
    });

    let mut user_id: u64 = 0;
    while let Some(sample) = gen.next() {
        users_per_sec.fetch_add(1, Ordering::SeqCst);
        user_id += 1;
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

            let intention = select_intention(&state, &products, &mut rng);
            if state.session_id > 0 {
                let add_time = match intention {
                    Intention::BuyCertainProduct(_) => Duration::weeks(2).num_seconds(),
                    Intention::BuyAnyProduct => Duration::weeks(2).num_seconds(),
                    Intention::JustBrowse => Duration::weeks(2).num_seconds(),
                    Intention::MakeRefund(_) => Duration::weeks(1).num_seconds(),
                };

                state.cur_timestamp += rng.gen_range(
                    add_time..=add_time + add_time / 10) as i64;
            }
            let mut coefficients = make_coefficients(&intention);
            if rng.gen::<f64>() < coefficients.global_bounce_rate {
                break 'session;
            }

            let mut transitions = make_transitions(&coefficients);

            let mut prev_action: Action = Action::None;
            let mut action = Action::ViewIndex;
            let mut wait_time = 0;
            // println!("session: #{}, intention: {:?}", state.session_id, intention);

            'events: loop {
                // println!("action: {action}");
                events_per_sec.fetch_add(1, Ordering::SeqCst);
                match (prev_action, action, intention) {
                    (_, Action::EndSession, Intention::JustBrowse | Intention::BuyAnyProduct | Intention::BuyCertainProduct(_)) => {
                        if state.cart.len() == 0 {
                            break 'events;
                        }

                        // coefficients.view_product_to_buy = 1.;
                        // transitions = make_transitions(&coefficients);
                        action = Action::ViewCart;
                    }
                    (_, Action::EndSession, _) => break 'events,
                    (Action::SearchProduct, Action::ViewProduct, Intention::BuyCertainProduct(product)) => {
                        state.search_query = Some(products.str_name(product.name));
                        state.selected_product = Some(product);
                    }
                    (Action::SearchProduct, Action::ViewProduct, _) => {
                        for (idx, product) in products.products.iter().enumerate() {
                            if state.products_viewed.contains_key(&product.id) {
                                continue;
                            }
                            if rng.gen::<f64>() < products.product_weights[idx] {
                                state.selected_product = Some(product);
                                state.search_query = Some(products.str_name(product.name));
                                break;
                            }
                        }

                        if state.selected_product.is_none() {
                            (prev_action, action) = (action, Action::EndSession)
                        }
                    }
                    (Action::ViewIndexPromotions, Action::ViewProduct, _) => {
                        let sp = products.promoted_product_sample(&mut rng);
                        if state.products_viewed.contains_key(&sp.id) {
                            action = Action::EndSession;
                            continue;
                        }
                        state.selected_product.insert(sp);
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
                    (Action::ViewDeals, Action::ViewProduct, _) => {
                        let sp = products.deal_product_sample(&mut rng);
                        if state.products_viewed.contains_key(&sp.id) {
                            action = Action::EndSession;
                            continue;
                        }
                        state.selected_product.insert(sp);
                        *state.products_viewed.entry(state.selected_product.unwrap().id).or_insert(0) += 1;
                    }
                    (_, Action::ViewRelatedProduct, _) => {
                        let product = &state.selected_product.unwrap();
                        let found = products.products
                            .iter()
                            .find(|p| p.category == product.category && p.id != product.id);

                        if found.is_none() {
                            // println!("no related product");
                            action = Action::EndSession;
                            continue;
                        }
                        state.selected_product = found;
                        *state.products_viewed.entry(state.selected_product.unwrap().id).or_insert(0) += 1;
                    }
                    (Action::ViewOrders, Action::RefundProduct, Intention::MakeRefund(product)) => {
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

                prev_action = action;
                (action, wait_time) = next_action(action, &transitions, &mut rng);

                if let Some(event) = prev_action.to_event() {
                    batch_builder.write_event(event, *events_map.get(&event).unwrap(), &state, &sample.profile)?;
                    if batch_builder.len() >= batch_size {
                        result[user_id as usize % partitions].push(batch_builder.to_record_batch()?);
                    }
                }

                match prev_action {
                    Action::CompleteOrder => {
                        state.cart.clear();
                    }
                    _ => {}
                }
                state.cur_timestamp += rng.gen_range(
                    wait_time..=wait_time + wait_time / 10) as i64;
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

    Ok(result)
}

pub fn next_action(from: Action, transitions: &Vec<(Action, Vec<(Action, f64, u64)>)>, rng: &mut ThreadRng) -> (Action, u64) {
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
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration as StdDuration;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common::DECIMAL_SCALE;
use crossbeam_channel::tick;
use crossbeam_channel::Sender;
use rand::prelude::*;
use rand::rngs::ThreadRng;
use rust_decimal::Decimal;
use tracing::info;

use crate::error::Result;
use crate::generator::Generator;
use crate::store::actions::Action;
use crate::store::coefficients::make_coefficients;
use crate::store::events::Event;
use crate::store::intention::select_intention;
use crate::store::intention::Intention;
use crate::store::products::Product;
use crate::store::products::ProductProvider;
use crate::store::profiles::Profile;
use crate::store::transitions::make_transitions;

#[derive(Debug, Clone)]
pub struct EventRecord {
    pub user_id: i64,
    pub created_at: i64,
    pub event: i64,
    pub page_path: String,
    pub page_search: String,
    pub page_title: String,
    pub page_url: String,
    pub a_name: String,
    pub a_href: String,
    pub product_name: Option<i16>,
    pub product_category: Option<i16>,
    pub product_subcategory: Option<i16>,
    pub product_brand: Option<i16>,
    pub product_price: Option<i128>,
    pub product_discount_price: Option<i128>,
    pub spent_total: Option<i128>,
    pub products_bought: Option<i8>,
    pub cart_items_number: Option<i8>,
    pub cart_amount: Option<i128>,
    pub revenue: Option<i128>,
    pub country: Option<i16>,
    pub city: Option<i16>,
    pub device: Option<i16>,
    pub device_category: Option<i16>,
    pub os: Option<i16>,
    pub os_version: Option<i16>,
}

pub struct State<'a> {
    pub session_id: usize,
    pub event_id: usize,
    pub user_id: i64,
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
    pub events_map: HashMap<Event, u64>,
    pub products: ProductProvider,
    pub to: DateTime<Utc>,
    pub out: Sender<Option<EventRecord>>,
}

pub struct Scenario {
    pub rng: ThreadRng,
    pub gen: Generator,
    pub events_map: HashMap<Event, u64>,
    pub products: ProductProvider,
    pub to: DateTime<Utc>,
    pub out: Sender<Option<EventRecord>>,
}

impl Scenario {
    pub fn new(cfg: Config) -> Self {
        Self {
            rng: cfg.rng,
            gen: cfg.gen,
            events_map: cfg.events_map,
            products: cfg.products,
            to: cfg.to,
            out: cfg.out,
        }
    }

    pub fn run(&mut self) -> Result<()> {
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
            }
        });

        let mut user_id: i64 = 0;
        let mut overall_events: usize = 0;
        while let Some(sample) = self.gen.next_sample() {
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

                    state.cur_timestamp += self.rng.gen_range(add_time..=add_time + add_time / 10);
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
                            state.search_query = Some(self.products.string_name(product.name)?);
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
                                        Some(self.products.string_name(product.name)?);
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
                        self.write_event(event, &state, &sample.profile);
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

        self.out.send(None).unwrap();

        Ok(())
    }

    fn write_event(&self, event: Event, state: &State, profile: &Profile) {
        let event_id = *self.events_map.get(&event).unwrap();
        let mut rec = EventRecord {
            user_id: state.user_id,
            created_at: state.cur_timestamp * 10i64.pow(3),
            event: event_id as i64,
            page_path: "".to_string(),
            page_search: "".to_string(),
            page_title: "".to_string(),
            page_url: "".to_string(),
            a_name: "".to_string(),
            a_href: "".to_string(),
            product_name: None,
            product_category: None,
            product_subcategory: None,
            product_brand: None,
            product_price: None,
            product_discount_price: None,
            spent_total: None,
            products_bought: None,
            cart_items_number: None,
            cart_amount: None,
            revenue: None,
            country: None,
            city: None,
            device: None,
            device_category: None,
            os: None,
            os_version: None,
        };
        match event {
            Event::UserRegistered => {
                rec.page_path = "register".to_string();
                rec.page_title = "user registration".to_string();
            }
            Event::UserLoggedIn => {
                rec.page_path = "login".to_string();
                rec.page_title = "user login".to_string();
            }
            Event::SubscribedForNewsletter => {
                rec.page_path = "subscribe".to_string();
                rec.page_title = "newsletter subscription".to_string();
            }
            Event::IndexPageViewed => {
                rec.page_path = "index".to_string();
                rec.page_title = "index page".to_string();
            }
            Event::DealsViewed => {
                rec.page_path = "deals".to_string();
                rec.page_title = "deals".to_string();
            }
            Event::ProductSearched => {
                rec.page_path = "search".to_string();
                rec.page_title = "search product".to_string();
                if let Some(query) = &state.search_query {
                    rec.page_search = query.to_owned();
                }
            }
            Event::NotFound => {
                rec.page_path = "not-found".to_string();
                rec.page_title = "404 not found".to_string();
            }
            Event::ProductViewed => {
                rec.page_path = state.selected_product.unwrap().path();
                rec.page_title = state.selected_product.unwrap().name_str.clone();
            }
            Event::ProductAddedToCart => {
                rec.page_path = state.selected_product.unwrap().path();
                rec.page_title = state.selected_product.unwrap().name_str.clone();
            }
            Event::BuyNowProduct => {
                rec.page_path = state.selected_product.unwrap().path();
                rec.page_title = state.selected_product.unwrap().name_str.clone();
            }
            Event::ProductRated => {
                rec.page_path = state.selected_product.unwrap().path();
                rec.page_title = state.selected_product.unwrap().name_str.clone();
            }
            Event::CartViewed => {
                rec.page_path = "cart".to_string();
                rec.page_title = "cart".to_string();
            }
            Event::CouponApplied => {
                rec.page_path = "coupons".to_string();
                rec.page_title = "coupons".to_string();
            }
            Event::CustomerInformationEntered => {
                rec.page_path = "checkout".to_string();
                rec.page_title = "checkout".to_string();
            }
            Event::ShippingMethodEntered => {
                rec.page_path = "checkout".to_string();
                rec.page_title = "checkout".to_string();
            }
            Event::PaymentMethodEntered => {
                rec.page_path = "checkout".to_string();
                rec.page_title = "checkout".to_string();
            }
            Event::OrderVerified => {}
            Event::OrderCompleted => {
                rec.page_path = "order-completed".to_string();
                rec.page_title = "orders".to_string();
            }
            Event::ProductRefunded => {}
            Event::OrdersViewed => {
                rec.page_path = "orders".to_string();
                rec.page_title = "my orders".to_string();
            }
        }

        if let Some(product) = state.selected_product {
            rec.product_name = Some(product.name as i16);
            rec.product_category = Some(product.category as i16);
            rec.product_subcategory = product.subcategory.map(|v| v as i16);
            rec.product_brand = product.brand.map(|v| v as i16);
            rec.product_price = Some(product.price.mantissa());

            if let Some(price) = product.discount_price {
                rec.product_discount_price = Some(price.mantissa());
            }
        }

        if !state.spent_total.is_zero() {
            rec.spent_total = Some(state.spent_total.mantissa());
        }

        if !state.products_bought.is_empty() {
            rec.products_bought = Some(state.products_bought.len() as i8);
        }

        let mut cart_amount: Option<Decimal> = None;
        if !state.cart.is_empty() {
            rec.cart_items_number = Some(state.cart.len() as i8);
            let cart_amount_: Decimal = state
                .cart
                .iter()
                .map(|p| p.discount_price.unwrap_or(p.price))
                .sum();

            rec.cart_amount = Some(cart_amount_.mantissa());
            cart_amount = Some(cart_amount_);
        }

        if event == Event::OrderCompleted {
            rec.revenue = Some(cart_amount.unwrap().mantissa());
        }

        rec.country = profile.geo.country.map(|v| v as i16);
        rec.city = profile.geo.city.map(|v| v as i16);
        rec.device = profile.device.device.map(|v| v as i16);
        rec.device_category = profile.device.device_category.map(|v| v as i16);
        rec.os = profile.device.os.map(|v| v as i16);
        rec.os_version = profile.device.os_version.map(|v| v as i16);

        self.out.send(Some(rec)).unwrap();
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

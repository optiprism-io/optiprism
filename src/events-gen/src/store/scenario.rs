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
use common::types;
use common::DECIMAL_SCALE;
use crossbeam_channel::tick;
use crossbeam_channel::Sender;
use ingester::executor::Executor;
use ingester::Context;
use ingester::Identify;
use ingester::Page;
use ingester::PropValue;
use ingester::PropertyAndValue;
use ingester::RequestContext;
use ingester::Track;
use metadata::properties::Properties;
use metadata::MetadataProvider;
use rand::prelude::*;
use rand::rngs::ThreadRng;
use rust_decimal::Decimal;
use tracing::info;

use crate::error::Result;
use crate::generator::Generator;
use crate::store::actions::Action;
use crate::store::coefficients::make_coefficients;
use crate::store::companies::CompanyProvider;
use crate::store::events::Event;
use crate::store::intention::select_intention;
use crate::store::intention::Intention;
use crate::store::products::Product;
use crate::store::products::ProductProvider;
use crate::store::profiles::Profile;
use crate::store::transitions::make_transitions;

pub struct State<'a> {
    pub session_id: usize,
    pub event_id: usize,
    pub user_id: i64,
    pub cur_timestamp: i64,
    pub selected_product: Option<&'a Product>,
    pub products_bought: HashMap<&'a Product, usize>,
    pub products_viewed: HashMap<&'a Product, usize>,
    pub products_refunded: HashMap<&'a Product, ()>,
    pub cart: Vec<&'a Product>,
    pub search_query: Option<&'a String>,
    pub spent_total: Decimal,
}

pub struct Config {
    pub rng: ThreadRng,
    pub gen: Generator,
    pub products: ProductProvider,
    pub to: DateTime<Utc>,
    pub track: Executor<Track>,
    pub identify: Executor<Identify>,
    pub user_props_prov: Arc<Properties>,
    pub project_id: u64,
    pub token: String,
}

pub struct Scenario {
    pub rng: ThreadRng,
    pub gen: Generator,
    pub products: ProductProvider,
    pub companies: CompanyProvider,
    pub to: DateTime<Utc>,
    pub track: Executor<Track>,
    pub identify: Executor<Identify>,
    pub props_prov: Arc<Properties>,
    pub project_id: u64,
    pub token: String,
}

impl Scenario {
    pub fn new(cfg: Config) -> Self {
        Self {
            rng: cfg.rng,
            gen: cfg.gen,
            products: cfg.products,
            to: cfg.to,
            track: cfg.track,
            identify: cfg.identify,
            props_prov: cfg.user_props_prov,
            project_id: cfg.project_id,
            token: cfg.token,
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
            let profile = &sample.profile;
            let company = self.companies.sample(&mut self.rng);
            let project = format!("Project {}", &mut self.rng.gen_range(1..=10));
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

                let rng = &mut self.rng;
                let intention = select_intention(&state, &self.products, rng);
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
                    match (prev_action, action, &intention) {
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
                            state.search_query = Some(&product.name);
                            state.selected_product = Some(product);
                        }
                        (Some(Action::SearchProduct), Action::ViewProduct, _) => {
                            for (idx, product) in self.products.products.iter().enumerate() {
                                if state
                                    .products_viewed
                                    .iter()
                                    .find(|(p, _)| p.name == product.name)
                                    .is_some()
                                {
                                    continue;
                                }
                                if self.rng.gen::<f64>() < self.products.product_weights[idx] {
                                    state.selected_product = Some(product);
                                    state.search_query = Some(&product.name);
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
                            if state
                                .products_viewed
                                .iter()
                                .find(|(p, _)| p.name == sp.name)
                                .is_some()
                            {
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
                                state
                                    .products_bought
                                    .iter_mut()
                                    .find(|(p, _)| p.name == product.name)
                                    .map(|(_, v)| *v += 1);
                                state.spent_total += product.final_price();
                            }
                        }
                        (Some(Action::ViewDeals), Action::ViewProduct, _) => {
                            let sp = self.products.deal_product_sample(&mut self.rng);
                            if state
                                .products_viewed
                                .iter()
                                .find(|(p, _)| p.name == sp.name)
                                .is_some()
                            {
                                action = Action::EndSession;
                                continue;
                            }
                            let _ = state.selected_product.insert(sp);
                            state
                                .products_viewed
                                .iter_mut()
                                .find(|(p, _)| p.name == state.selected_product.unwrap().name)
                                .map(|(_, v)| *v += 1);
                        }
                        (_, Action::ViewRelatedProduct, _) => {
                            let product = &state.selected_product.unwrap();
                            let found =
                                self.products.products.iter().find(|p| {
                                    p.category == product.category && p.name != product.name
                                });

                            if found.is_none() {
                                action = Action::EndSession;
                                continue;
                            }
                            state.selected_product = found;
                            state
                                .products_viewed
                                .iter_mut()
                                .find(|(p, _)| p.name == state.selected_product.unwrap().name)
                                .map(|(_, v)| *v += 1);
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
                        self.write_event(event, &state, profile)?;
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

        Ok(())
    }

    fn write_group(&self) {}
    fn write_event(&self, event: Event, state: &State, profile: &Profile) -> Result<()> {
        let mut page = Page {
            path: None,
            referrer: None,
            search: None,
            title: None,
            url: None,
        };

        match &event {
            Event::UserRegistered => {
                page.path = Some("register".to_string());
                page.title = Some("user registration".to_string());
            }
            Event::UserLoggedIn => {
                page.path = Some("login".to_string());
                page.title = Some("user login".to_string());
            }
            Event::SubscribedForNewsletter => {
                page.path = Some("subscribe".to_string());
                page.title = Some("newsletter subscription".to_string());
            }
            Event::IndexPageViewed => {
                page.path = Some("index".to_string());
                page.title = Some("index page".to_string());
            }
            Event::DealsViewed => {
                page.path = Some("deals".to_string());
                page.title = Some("deals".to_string());
            }
            Event::ProductSearched => {
                page.path = Some("search".to_string());
                page.title = Some("search product".to_string());
                if let Some(query) = state.search_query {
                    page.search = Some(query.to_owned());
                }
            }
            Event::NotFound => {
                page.path = Some("not-found".to_string());
                page.title = Some("404 not found".to_string());
            }
            Event::ProductViewed => {
                page.path = Some(state.selected_product.unwrap().path());
                page.title = Some(state.selected_product.unwrap().name.clone());
            }
            Event::ProductAddedToCart => {
                page.path = Some(state.selected_product.unwrap().path());
                page.title = Some(state.selected_product.unwrap().name.clone());
            }
            Event::BuyNowProduct => {
                page.path = Some(state.selected_product.unwrap().path());
                page.title = Some(state.selected_product.unwrap().name.clone());
            }
            Event::ProductRated => {
                page.path = Some(state.selected_product.unwrap().path());
                page.title = Some(state.selected_product.unwrap().name.clone());
            }
            Event::CartViewed => {
                page.path = Some("cart".to_string());
                page.title = Some("cart".to_string());
            }
            Event::CouponApplied => {
                page.path = Some("coupons".to_string());
                page.title = Some("coupons".to_string());
            }
            Event::CustomerInformationEntered => {
                page.path = Some("checkout".to_string());
                page.title = Some("checkout".to_string());
            }
            Event::ShippingMethodEntered => {
                page.path = Some("checkout".to_string());
                page.title = Some("checkout".to_string());
            }
            Event::PaymentMethodEntered => {
                page.path = Some("checkout".to_string());
                page.title = Some("checkout".to_string());
            }
            Event::OrderVerified => {}
            Event::OrderCompleted => {
                page.path = Some("order-completed".to_string());
                page.title = Some("orders".to_string());
            }
            Event::ProductRefunded => {}
            Event::OrdersViewed => {
                page.path = Some("orders".to_string());
                page.title = Some("my orders".to_string());
            }
        }

        let context = Context {
            library: None,
            page: Some(page),
            user_agent: None,
            ip: profile.ip.clone(),
        };

        let mut properties = HashMap::default();
        if let Some(product) = state.selected_product {
            properties.insert(
                "Product Name".to_string(),
                PropValue::String(product.name.clone()),
            );
            properties.insert(
                "Product Category".to_string(),
                PropValue::String(product.category.clone()),
            );
            if let Some(subcategory) = &product.subcategory {
                properties.insert(
                    "Product Subcategory".to_string(),
                    PropValue::String(subcategory.clone()),
                );
            }
            if let Some(brand) = &product.brand {
                properties.insert(
                    "Product Brand".to_string(),
                    PropValue::String(brand.clone()),
                );
            }
            properties.insert(
                "Product Price".to_string(),
                PropValue::Number(product.price),
            );
            if let Some(price) = product.discount_price {
                properties.insert(
                    "Product Discount Price".to_string(),
                    PropValue::Number(price),
                );
            }
        }

        if !state.spent_total.is_zero() {
            properties.insert(
                "Spent Total".to_string(),
                PropValue::Number(state.spent_total.clone()),
            );
        }
        if !state.products_bought.is_empty() {
            properties.insert(
                "Products Bought".to_string(),
                PropValue::Number(Decimal::new(
                    state.products_bought.len() as i64,
                    DECIMAL_SCALE as u32,
                )),
            );
        }
        let mut cart_amount: Option<Decimal> = None;
        if !state.cart.is_empty() {
            properties.insert(
                "Cart Items Number".to_string(),
                PropValue::Number(Decimal::new(state.cart.len() as i64, DECIMAL_SCALE as u32)),
            );
            let cart_amount_: Decimal = state
                .cart
                .iter()
                .map(|p| p.discount_price.unwrap_or(p.price))
                .sum();

            properties.insert(
                "Cart Amount".to_string(),
                PropValue::Number(cart_amount_.clone()),
            );
            cart_amount = Some(cart_amount_);
        }

        if event == Event::OrderCompleted {
            properties.insert(
                "Revenue".to_string(),
                PropValue::Number(cart_amount.unwrap()),
            );
        }

        let mut user_props = vec![];
        if let Some(country) = &profile.geo.country {
            let prop = self
                .props_prov
                .get_by_name(self.project_id, types::EVENT_PROPERTY_COUNTRY)?;

            let prop = PropertyAndValue {
                property: prop,
                value: PropValue::String(country.to_owned()),
            };
            user_props.push(prop);
        }
        if let Some(city) = &profile.geo.city {
            let prop = self
                .props_prov
                .get_by_name(self.project_id, types::EVENT_PROPERTY_CITY)?;

            let prop = PropertyAndValue {
                property: prop,
                value: PropValue::String(city.to_owned()),
            };
            user_props.push(prop);
        }
        if let Some(device) = &profile.device.device {
            let prop = self
                .props_prov
                .get_by_name(self.project_id, types::EVENT_PROPERTY_DEVICE_MODEL)?;

            let prop = PropertyAndValue {
                property: prop,
                value: PropValue::String(device.to_owned()),
            };
            user_props.push(prop);
        }
        if let Some(device_category) = &profile.device.device_category {
            let prop = self
                .props_prov
                .get_by_name(self.project_id, types::EVENT_PROPERTY_OS_FAMILY)?;

            let prop = PropertyAndValue {
                property: prop,
                value: PropValue::String(device_category.to_owned()),
            };
            user_props.push(prop);
        }
        if let Some(os) = &profile.device.os {
            let prop = self
                .props_prov
                .get_by_name(self.project_id, types::EVENT_PROPERTY_OS)?;

            let prop = PropertyAndValue {
                property: prop,
                value: PropValue::String(os.to_owned()),
            };
            user_props.push(prop);
        }
        if let Some(os_version) = &profile.device.os_version {
            let prop = self
                .props_prov
                .get_by_name(self.project_id, types::EVENT_PROPERTY_OS_VERSION_MAJOR)?;

            let prop = PropertyAndValue {
                property: prop,
                value: PropValue::String(os_version.to_owned()),
            };
            user_props.push(prop);
        }

        let req = Track {
            user_id: Some(profile.email.clone()),
            anonymous_id: None,
            resolved_user_id: None,
            timestamp: DateTime::from_timestamp_millis(state.cur_timestamp * 10i64.pow(3)).unwrap(),
            context,
            event: event.to_string(),
            resolved_event: None,
            properties: Some(properties),
            resolved_properties: None,
            resolved_user_properties: Some(user_props),
            groups: None,
            resolved_groups: None,
        };
        let req_ctx = RequestContext {
            project_id: Some(self.project_id),
            client_ip: profile.ip.clone(),
            token: self.token.clone(),
        };

        self.track.execute(&req_ctx, req).map_err(|err| err.into())
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

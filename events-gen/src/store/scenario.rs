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
use strum_macros::{Display as EnumDisplay, EnumString};
// use crate::actions::{Actions, Probability, TransitionState};
use crate::probability;
use crate::store::products::{Dict, Preferences, Product, Products};
use rand::prelude::*;
use crate::generator::Generator;
use crossbeam_channel::tick;
use common::{DECIMAL_PRECISION, DECIMAL_SCALE};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;

#[derive(Debug, Clone, Copy, EnumDisplay)]
pub enum Event {
    UserRegistered = 1,
    UserLoggedIn = 2,
    SubscribedForNewsletter = 3,
    IndexPageViewed = 4,
    DealsViewed = 5,
    ProductSearched = 6,
    NotFound = 7,
    ProductViewed = 8,
    // add cross sell as well
    ProductAddedToCart = 9,
    BuyNowProduct = 10,
    ProductRated = 11,
    // 1
    CartViewed = 12,
    // 1.1
    CouponApplied = 13,
    // 2
    CustomerInformationEntered = 14,
    // 3
    ShippingMethodEntered = 15,
    // 4
    PaymentMethodEntered = 16,
    // 5
    OrderVerified = 17,
    // 6
    OrderCompleted = 18,
    ProductRefunded = 19,
    OrdersViewed = 20,
    AbandonCart = 21,
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, EnumDisplay)]
pub enum Action {
    ViewIndex,
    ViewIndexPromotions,
    ViewDeals,
    SearchProduct,
    ViewRelatedProduct,
    NotFound,
    ViewProduct,
    AddProductToCart,
    // ViewCartAndPay,
    ViewCart,
    EnterCustomerInformation,
    EnterShippingMethod,
    EnterPaymentMethod,
    VerifyOrder,
    CompleteOrder,
    RefundProduct,
    ViewOrders,
    Bounce,
    AbandonCart,
    EndSession,
    None, // special case
}

impl Action {
    pub fn to_event(&self) -> Option<Event> {
        match self {
            Action::ViewIndex => Some(Event::IndexPageViewed),
            Action::ViewDeals => Some(Event::DealsViewed),
            Action::SearchProduct => Some(Event::ProductSearched),
            Action::NotFound => Some(Event::NotFound),
            Action::ViewRelatedProduct => Some(Event::ProductViewed),
            Action::ViewProduct => Some(Event::ProductViewed),
            Action::AddProductToCart => Some(Event::ProductAddedToCart),
            Action::ViewCart => Some(Event::CartViewed),
            Action::EnterCustomerInformation => Some(Event::CustomerInformationEntered),
            Action::EnterShippingMethod => Some(Event::ShippingMethodEntered),
            Action::EnterPaymentMethod => Some(Event::PaymentMethodEntered),
            Action::VerifyOrder => Some(Event::OrderVerified),
            Action::CompleteOrder => Some(Event::OrderCompleted),
            Action::RefundProduct => Some(Event::ProductRefunded),
            Action::ViewOrders => Some(Event::OrdersViewed),
            _ => None,
        }
    }
}

struct NullVec<T> {
    vec: Vec<T>,
    nulls: Vec<bool>,
}

impl<T> NullVec<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            vec: Vec::with_capacity(capacity),
            nulls: Vec::with_capacity(capacity),
        }
    }

    pub fn clear(&mut self) {
        self.vec.clear();
        self.nulls.clear();
    }
}

pub struct RecordBatchBuilder {
    created_at: TimestampSecondBuilder,
    user_id: UInt64Builder,
    event: UInt16Builder,
    product_name: UInt16Builder,
    product_category: UInt16Builder,
    product_subcategory: UInt16Builder,
    product_brand: UInt16Builder,
    product_price: DecimalBuilder,
    product_discount_price: DecimalBuilder,
    // search_query: UInt16Builder,
    spent_total: DecimalBuilder,
    products_bought: UInt8Builder,
    cart_items_number: UInt8Builder,
    cart_amount: DecimalBuilder,
    revenue: DecimalBuilder,
    schema: SchemaRef,
    len: usize,
}

impl RecordBatchBuilder {
    pub fn new(cap: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("created_at", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new("user_id", DataType::UInt64, false),
            Field::new("event", DataType::UInt16, false),
            Field::new("product_name", DataType::UInt16, true),
            Field::new("product_category", DataType::UInt16, true),
            Field::new("product_subcategory", DataType::UInt16, true),
            Field::new("product_brand", DataType::UInt16, true),
            Field::new("product_price", DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE), true),
            Field::new("product_discount_price", DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE), true),
            // Field::new("search_query", DataType::UInt16, true),
            Field::new("spent_total", DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE), true),
            Field::new("products_bought", DataType::UInt8, true),
            Field::new("cart_items_number", DataType::UInt8, true),
            Field::new("cart_amount", DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE), true),
            Field::new("revenue", DataType::Decimal(DECIMAL_PRECISION, DECIMAL_SCALE), true),
        ]));

        Self {
            created_at: TimestampSecondBuilder::new(cap),
            user_id: UInt64Builder::new(cap),
            event: UInt16Builder::new(cap),
            product_name: UInt16Builder::new(cap),
            product_category: UInt16Builder::new(cap),
            product_subcategory: UInt16Builder::new(cap),
            product_brand: UInt16Builder::new(cap),
            product_price: DecimalBuilder::new(cap, DECIMAL_PRECISION, DECIMAL_SCALE),
            product_discount_price: DecimalBuilder::new(cap, DECIMAL_PRECISION, DECIMAL_SCALE),
            // search_query: UInt16Builder::new(cap),
            spent_total: DecimalBuilder::new(cap, DECIMAL_PRECISION, DECIMAL_SCALE),
            products_bought: UInt8Builder::new(cap),
            cart_items_number: UInt8Builder::new(cap),
            cart_amount: DecimalBuilder::new(cap, DECIMAL_PRECISION, DECIMAL_SCALE),
            revenue: DecimalBuilder::new(cap, DECIMAL_PRECISION, DECIMAL_SCALE),
            schema,
            len: 0,
        }
    }

    pub fn to_record_batch(&mut self) -> Result<RecordBatch> {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.created_at.finish()),
            Arc::new(self.user_id.finish()),
            Arc::new(self.event.finish()),
            Arc::new(self.product_name.finish()),
            Arc::new(self.product_category.finish()),
            Arc::new(self.product_subcategory.finish()),
            Arc::new(self.product_brand.finish()),
            Arc::new(self.product_price.finish()),
            Arc::new(self.product_discount_price.finish()),
            Arc::new(self.spent_total.finish()),
            Arc::new(self.products_bought.finish()),
            Arc::new(self.cart_items_number.finish()),
            Arc::new(self.cart_amount.finish()),
            Arc::new(self.revenue.finish()),
        ];

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            cols,
        )?;

        self.len = 0;
        Ok(batch)
    }

    pub fn write_event(&mut self, action: Action, state: &State) -> Result<()> {
        let maybe_event = action.to_event();
        if maybe_event.is_none() {
            return Ok(());
        }

        let event = maybe_event.unwrap();

        // println!("event: {event}, time: {}", NaiveDateTime::from_timestamp(state.cur_timestamp, 0));
        self.created_at.append_value(state.cur_timestamp);
        self.user_id.append_value(state.user_id);
        self.event.append_value(event as u16);

        match state.selected_product {
            None => {
                self.product_name.append_null()?;
                self.product_category.append_null()?;
                self.product_subcategory.append_null()?;
                self.product_brand.append_null()?;
                self.product_price.append_null()?;
                self.product_discount_price.append_null()?;
            }
            Some(product) => {
                self.product_name.append_value(product.name as u16)?;
                self.product_category.append_value(product.category as u16)?;
                self.product_subcategory.append_option(product.subcategory.map(|v| v as u16))?;
                self.product_brand.append_option(product.brand.map(|v| v as u16))?;
                self.product_price.append_value(product.price.mantissa())?;

                match product.discount_price {
                    None => self.product_discount_price.append_null()?,
                    Some(price) => {
                        self.product_discount_price.append_value(price.mantissa())?;
                    }
                }
            }
        }

        if !state.spent_total.is_zero() {
            self.spent_total.append_value(state.spent_total.mantissa())?;
        } else {
            self.spent_total.append_null()?;
        }

        if state.products_bought.len() > 0 {
            self.products_bought.append_value(state.products_bought.len() as u8)?;
        } else {
            self.products_bought.append_null()?;
        }

        let mut cart_amount: Option<Decimal> = None;
        if state.cart.len() > 0 {
            self.cart_items_number.append_value(state.cart.len() as u8)?;
            let mut _cart_amount: Decimal = state.cart
                .iter()
                .map(|p| p.discount_price.unwrap_or(p.price))
                .sum();

            self.cart_amount.append_value(_cart_amount.mantissa())?;
            cart_amount = Some(_cart_amount);
        } else {
            self.cart_items_number.append_null()?;
            self.cart_amount.append_null();
        }

        match event {
            Event::OrderCompleted => {
                self.revenue.append_value(cart_amount.unwrap().mantissa())?;
            }
            _ => {
                self.revenue.append_null()?;
            }
        }

        self.len += 1;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

pub fn select_intention<'a>(state: &State, products: &'a Products, rng: &mut ThreadRng) -> Intention<'a> {
    if state.session_id > 0 && !state.products_bought.is_empty() && rng.gen::<f64>() < 0.1 {
        for (id, _) in state.products_bought.iter() {
            if rng.gen::<f64>() < 0.5 && !state.products_refunded.contains_key(id) {
                return Intention::MakeRefund(&products.products[*id]);
            }
        }
    }

    if state.session_id == 0 {
        if rng.gen::<f64>() < 0.8 {
            return Intention::JustBrowse;
        }
    } else {
        if rng.gen::<f64>() < 0.05 {
            return Intention::JustBrowse;
        }
    }


    if rng.gen::<f64>() < 0.2 {
        return Intention::BuyCertainProduct(products.product_sample(rng));
    }

    Intention::BuyAnyProduct
}

pub fn make_coefficients(intention: &Intention) -> Coefficients {
    match intention {
        Intention::BuyCertainProduct(_) => Coefficients {
            abandon_cart: 0.1,
            cart_completion: 0.9,
            discover: 0.7,
            search_for_product: 0.0,
            go_to_index: 0.1,
            store_navigation_quality: 0.9,
            bounce_rate: 0.1,
            global_bounce_rate: 0.1,
            buy_multiple_products: 0.0,
            search_quality: 0.9,
            view_product_to_buy: 0.5,
            refund: 0.0,
            product_rating: 0.9,
        },
        Intention::BuyAnyProduct => Coefficients {
            abandon_cart: 0.1,
            cart_completion: 0.9,
            discover: 0.7,
            search_for_product: 0.0,
            go_to_index: 0.1,
            store_navigation_quality: 0.9,
            bounce_rate: 0.1,
            global_bounce_rate: 0.1,
            buy_multiple_products: 0.1,
            search_quality: 0.9,
            view_product_to_buy: 0.5,
            refund: 0.0,
            product_rating: 0.9,
        },
        Intention::JustBrowse => Coefficients {
            abandon_cart: 0.1,
            cart_completion: 0.9,
            discover: 0.7,
            search_for_product: 0.0,
            go_to_index: 0.1,
            store_navigation_quality: 0.9,
            bounce_rate: 0.1,
            global_bounce_rate: 0.1,
            buy_multiple_products: 0.1,
            search_quality: 0.9,
            view_product_to_buy: 0.1,
            refund: 0.0,
            product_rating: 0.9,
        },
        Intention::MakeRefund(_) => Coefficients {
            abandon_cart: 0.,
            cart_completion: 0.,
            discover: 0.,
            search_for_product: 0.,
            go_to_index: 0.1,
            store_navigation_quality: 0.,
            bounce_rate: 0.,
            global_bounce_rate: 0.1,
            buy_multiple_products: 0.,
            search_quality: 0.,
            view_product_to_buy: 0.,
            refund: 1.0,
            product_rating: 0.,
        }
    }
}

#[derive(Clone, Copy, Debug, EnumDisplay)]
pub enum Intention<'a> {
    BuyCertainProduct(&'a Product),
    BuyAnyProduct,
    JustBrowse,
    MakeRefund(&'a Product),
}

pub struct State<'a> {
    session_id: usize,
    event_id: usize,
    user_id: u64,
    cur_timestamp: i64,
    selected_product: Option<&'a Product>,
    products_bought: HashMap<usize, usize>,
    products_viewed: HashMap<usize, usize>,
    products_refunded: HashMap<usize, ()>,
    cart: Vec<&'a Product>,
    search_query: Option<&'a str>,
    spent_total: Decimal,
}

pub fn run(mut rng: ThreadRng, gen: &mut Generator, products: &mut Products, to: DateTime<Utc>, batch_size: usize) -> Result<Vec<Vec<RecordBatch>>> {
    let mut result: Vec<Vec<RecordBatch>> = vec![Vec::with_capacity(1000); 32];
    let mut batch_builder = RecordBatchBuilder::new(batch_size);
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
                            state.spent_total += product.final_price(false);
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
                        state.spent_total -= product.final_price(false);
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
                batch_builder.write_event(prev_action, &state)?;
                if batch_builder.len() >= batch_size {
                    result[user_id as usize % 32].push(batch_builder.to_record_batch()?);
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
                    // println!("max ts");
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

pub struct Coefficients {
    pub abandon_cart: f64,
    pub cart_completion: f64,
    pub discover: f64,
    pub search_for_product: f64,
    pub go_to_index: f64,
    pub store_navigation_quality: f64,
    pub bounce_rate: f64,
    pub global_bounce_rate: f64,
    pub buy_multiple_products: f64,
    pub search_quality: f64,
    pub view_product_to_buy: f64,
    pub refund: f64,
    pub product_rating: f64,
}

pub fn make_transitions(coef: &Coefficients) -> Vec<(Action, Vec<(Action, f64, u64)>)> {
    vec![
        (
            Action::ViewIndex,
            vec![
                (Action::ViewIndexPromotions, coef.discover, 3),
                (Action::SearchProduct, coef.discover * 0.8, 15),
                (Action::SearchProduct, coef.search_for_product, 6),
                (Action::ViewDeals, coef.discover * 0.7, 8),
                (Action::Bounce, coef.bounce_rate, 5),
                (Action::ViewOrders, coef.refund, 7),
            ]
        ),
        (
            Action::ViewIndexPromotions,
            vec![
                (Action::ViewProduct, coef.discover, 10),
                (Action::ViewIndex, coef.go_to_index, 0),
            ]
        ),
        (
            Action::ViewDeals,
            vec![
                (Action::ViewProduct, coef.discover, 10),
                (Action::ViewIndex, coef.go_to_index, 5),
                (Action::Bounce, coef.bounce_rate, 5),
            ]
        ),
        (
            Action::ViewOrders,
            vec![
                (Action::RefundProduct, coef.refund, 10),
            ]
        ),
        (
            Action::RefundProduct,
            vec![
                (Action::EndSession, 1., 0),
            ]
        ),
        (
            Action::SearchProduct,
            vec![
                (Action::ViewProduct, coef.discover, 15),
                (Action::NotFound, coef.search_quality * 0.2, 4),
                (Action::ViewIndex, coef.go_to_index, 5),
                (Action::Bounce, coef.bounce_rate, 5),
            ]
        ),
        (
            Action::NotFound,
            vec![
                (Action::SearchProduct, coef.discover, 0),
                (Action::ViewIndex, coef.go_to_index, 0),
                (Action::Bounce, coef.bounce_rate * 1.1, 0),
            ]
        ),
        (
            Action::ViewProduct,
            vec![
                (Action::ViewIndex, coef.go_to_index, 5),
                (Action::ViewRelatedProduct, coef.discover * 0.5, 20),
                (Action::AddProductToCart, coef.view_product_to_buy, 30),
                (Action::Bounce, coef.bounce_rate, 5),
            ]
        ),
        (
            Action::ViewRelatedProduct,
            vec![
                (Action::ViewIndex, coef.go_to_index, 5),
                (Action::ViewRelatedProduct, coef.discover * 0.2, 20),
                (Action::AddProductToCart, coef.view_product_to_buy * 0.9, 30),
                (Action::Bounce, coef.bounce_rate, 5),
            ]
        ),
        (
            Action::AddProductToCart,
            vec![
                (Action::SearchProduct, coef.buy_multiple_products, 6),
                (Action::ViewIndex, coef.buy_multiple_products, 5),
                (Action::ViewRelatedProduct, coef.discover, 10),
                (Action::ViewCart, coef.view_product_to_buy, 5),
                (Action::AbandonCart, coef.abandon_cart, 5),
            ]
        ),
        (
            Action::ViewCart,
            vec![
                (Action::EnterCustomerInformation, coef.view_product_to_buy, 5),
                (Action::AbandonCart, coef.abandon_cart * 0.9, 5),
            ],
        ),
        (
            Action::EnterCustomerInformation,
            vec![
                (Action::EnterShippingMethod, coef.cart_completion, 20),
                (Action::AbandonCart, coef.abandon_cart * 0.8, 5),
            ]
        ),
        (
            Action::EnterShippingMethod,
            vec![
                (Action::EnterPaymentMethod, coef.cart_completion, 15),
                (Action::AbandonCart, coef.abandon_cart * 0.7, 5),
            ]
        ),
        (
            Action::EnterPaymentMethod,
            vec![
                (Action::VerifyOrder, coef.cart_completion, 20),
                (Action::AbandonCart, coef.abandon_cart * 0.6, 5),
            ]
        ),
        (
            Action::VerifyOrder,
            vec![
                (Action::CompleteOrder, coef.cart_completion, 10),
                (Action::AbandonCart, coef.abandon_cart * 0.5, 5),
            ]
        ),
        (
            Action::CompleteOrder,
            vec![
                (Action::EndSession, 1., 10)
            ]
        ),
    ]
}
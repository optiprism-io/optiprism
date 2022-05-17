use std::collections::HashMap;
use std::fmt;
use std::path::Display;
use chrono::{DateTime, Duration, Utc};
use crate::error::{Result};
use std::string::ToString;
use rand::distributions::WeightedIndex;
use rand::rngs::ThreadRng;
use strum_macros::{Display, EnumString};
// use crate::actions::{Actions, Probability, TransitionState};
use crate::probability;
use crate::store::products::{Preferences, Product, Products};
use rand::prelude::*;

#[derive(Debug, Clone, Copy, Display)]
pub enum Event {
    UserRegistered,
    UserLoggedIn,
    SubscribedForNewsletter,
    IndexPageViewed,
    DealsViewed,
    ProductSearched,
    ProductViewed,
    // add cross sell as well
    ProductAddedToCart,
    BuyNowProduct,
    ProductRated,
    // 1
    CartViewed,
    // 1.1
    CouponApplied,
    // 2
    CustomerInformationEntered,
    // 3
    ShippingMethodEntered,
    // 4
    PaymentMethodEntered,
    // 5
    OrderVerified,
    // 6
    OrderCompleted,
    ProductRefunded,
    OrdersViewed,
    AbandonCart,
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Display)]
pub enum Action {
    ViewIndex,
    ViewIndexPromotions,
    ViewDeals,
    SearchProduct,
    ViewRelatedProduct,
    NotFound,
    ViewProduct,
    AddProductToCart,
    ViewCartAndPay,
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
}

fn write_event(event: Event) -> Result<()> {
    Ok(())
}

pub struct Time {
    rng: ThreadRng,
    t: DateTime<Utc>,
}

impl Time {
    pub fn new(t: DateTime<Utc>, rng: ThreadRng) -> Self {
        Self {
            rng,
            t,
        }
    }
    pub fn wait_between(&mut self, from: Duration, to: Duration) {
        let wait = Duration::seconds(self.rng.gen_range(from.num_seconds()..=to.num_seconds()));
        self.t = self.t.clone() + wait;
    }
}

macro_rules! write_event {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
    }};
}

#[derive(Clone, Copy, Debug)]
enum Intention<'a> {
    BuyCertainProduct(&'a Product),
    BuyAnyProduct,
    JustBrowse,
    MakeRefund(&'a Product),
}


pub fn make_state(transitions: &Vec<(Action, Vec<(Action, Vec<f64>)>)>) -> Vec<Vec<usize>> {
    transitions.iter().map(|(from, v)| vec![0; v.len()]).collect()
}

pub fn run(preferences: Preferences, time: &mut Time, products: &mut Products, rng: &mut ThreadRng) -> Result<()> {
    let mut selected_product: Option<&Product> = None;
    let mut products_bought: HashMap<usize, usize> = HashMap::default();
    let mut products_viewed: HashMap<usize, usize> = HashMap::default();
    let mut cart: Vec<&Product> = vec![];
    let mut spent_total: f64 = 0.;
    let rating_weights = probability::calc_cubic_spline(50, vec![0.01, 0.01, 0.1, 0.7, 1.])?;
    let product_popularity = probability::calc_cubic_spline(products.products.len(), vec![1., 0.8, 0.3, 0.1])?;

    let mut i = 0;
    'session: loop {
        println!("session {i}");
        let mut search_query: Option<&str> = None;
        let mut intention = Intention::BuyAnyProduct;
        let mut coefficients = Coefficients {
            abandon_cart: 0.1,
            cart_completion: 0.9,
            discover: 0.7,
            search_for_product: 0.0,
            go_to_index: 0.1,
            store_navigation_quality: 0.9,
            bounce_rate: 0.1,
            buy_multiple_products: 0.0,
            search_quality: 0.9,
            view_product_to_buy: 0.1,
            refund: 0.0,
            product_rating: 0.9,
        };

        let mut transitions = make_transitions(&coefficients);

        i += 1;
        if i > 10 {
            break;
        }

        let mut action = Action::ViewIndex;
        'events: loop {
            println!("action: {:?}", action);
            match action {
                Action::EndSession => {
                    if cart.len() == 0 {
                        break;
                    }

                    if rng.gen::<f64>() < coefficients.abandon_cart {
                        break;
                    }

                    action = Action::ViewCartAndPay;
                }
                Action::ViewIndex => {
                    write_event(Event::IndexPageViewed)?;
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    action = next_action(action, &transitions, rng);
                }
                Action::SearchProduct => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    action = next_action(action, &transitions, rng);
                    // TODO move to a common matcher of prev-next action pairs
                    match action {
                        Action::ViewProduct => {
                            match intention {
                                Intention::BuyCertainProduct(product) => {
                                    search_query = Some(product.name.as_str());
                                    selected_product = Some(product);
                                }
                                Intention::BuyAnyProduct => {
                                    for (idx, product) in products.products.iter().enumerate() {
                                        if products_viewed.contains_key(&product.id) {
                                            continue;
                                        }
                                        if rng.gen::<f64>() < product_popularity[idx] {
                                            selected_product = Some(product);
                                            break;
                                        }
                                    }

                                    if selected_product.is_none() {
                                        action = Action::EndSession
                                    }
                                }
                                _ => unimplemented!(),
                            }
                        }
                        Action::NotFound => {
                            action = next_action(Action::NotFound, &transitions, rng);
                        }
                        Action::EndSession => {}
                        // _ => unreachable!("{action}"),
                        _=>{}
                    }

                    write_event(Event::ProductSearched)?;
                }
                Action::ViewIndexPromotions => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    action = next_action(action, &transitions, rng);
                    if action != Action::ViewProduct {
                        continue;
                    }

                    let sp = products.promoted_product_sample(rng);
                    if products_viewed.contains_key(&sp.id) {
                        action = Action::EndSession;
                        continue;
                    }
                    selected_product.insert(sp);
                    action = next_action(action, &transitions, rng);
                }
                Action::ViewProduct => {
                    write_event(Event::ProductViewed)?;
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    *products_viewed.entry(selected_product.unwrap().id).or_insert(0) += 1;

                    let buy_coefficient = selected_product.unwrap().calc_buy_coefficient(&preferences, &rating_weights);
                    coefficients.view_product_to_buy = selected_product.unwrap().calc_buy_coefficient(&preferences, &rating_weights);
                    // transitions = transitions_fn(&coefficients);
                    action = next_action(action, &transitions, rng)
                }
                Action::AddProductToCart => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::ProductAddedToCart)?;
                    cart.push(selected_product.unwrap());
                    action = next_action(action, &transitions, rng);
                }
                Action::ViewCartAndPay => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::CartViewed)?;

                    action = next_action(action, &transitions, rng);
                }
                Action::ViewCart => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::CartViewed)?;

                    action = next_action(action, &transitions, rng);
                }
                Action::EnterCustomerInformation => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::CustomerInformationEntered)?;

                    action = next_action(action, &transitions, rng);
                }
                Action::EnterShippingMethod => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::ShippingMethodEntered)?;

                    action = next_action(action, &transitions, rng);
                }
                Action::EnterPaymentMethod => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::PaymentMethodEntered)?;

                    action = next_action(action, &transitions, rng);
                }
                Action::VerifyOrder => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::OrderVerified)?;

                    action = next_action(action, &transitions, rng);
                }
                Action::CompleteOrder => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::OrderCompleted)?;

                    for product in cart.iter() {
                        *products_bought.entry(product.id).or_insert(0) += 1;
                        spent_total += product.final_price(preferences.has_coupon);
                    }

                    cart.truncate(0);
                    action = next_action(action, &transitions, rng);
                }
                Action::RefundProduct => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::ProductRefunded)?;
                    spent_total -= selected_product.unwrap().final_price(preferences.has_coupon);
                    action = next_action(action, &transitions, rng);
                }
                Action::ViewDeals => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::DealsViewed)?;

                    action = next_action(action, &transitions, rng);
                    match action {
                        Action::ViewProduct => {
                            let sp = products.deal_product_sample(rng);
                            if products_viewed.contains_key(&sp.id) {
                                action = Action::EndSession;
                                continue;
                            }
                            selected_product.insert(sp);
                        }
                        _ => {}
                    }
                }
                Action::NotFound => {
                    unimplemented!()
                }
                Action::ViewRelatedProduct => {
                    let product = &selected_product.unwrap();
                    let found = products.products
                        .iter()
                        .find(|p| p.category == product.category && p.id != product.id);

                    if found.is_none() {
                        action = Action::EndSession;
                        continue;
                    }

                    action = Action::ViewProduct;
                }
                Action::ViewOrders => {
                    time.wait_between(Duration::seconds(10), Duration::seconds(30));
                    write_event(Event::OrdersViewed)?;

                    action = next_action(action, &transitions, rng);

                    match action {
                        Action::RefundProduct => {
                            if let Intention::MakeRefund(prod) = intention {
                                selected_product = Some(prod);
                            } else {
                                unreachable!();
                            }
                        }
                        _ => {}
                    }
                }
                Action::Bounce => {
                    break;
                }
                Action::AbandonCart => {
                    break;
                }
            }
        }
        println!();
    }

    Ok(())
}

pub fn next_action(from: Action, transitions: &Vec<(Action, Vec<(Action, f64)>)>, rng: &mut ThreadRng) -> Action {
    for (t_from, to) in transitions.iter() {
        if *t_from != from {
            continue;
        }

        let mut total_weights: f64 = 0.;
        for (_, weight) in to.iter() {
            total_weights += weight;
        }

        total_weights *= rng.gen::<f64>();
        for (t_to, weight) in to.iter() {
            total_weights -= weight;
            if total_weights < 0. {
                return t_to.clone();
            }
        }
    }
    unreachable!()
}

pub struct Coefficients {
    pub abandon_cart: f64,
    pub cart_completion: f64,
    pub discover: f64,
    pub search_for_product: f64,
    pub go_to_index: f64,
    pub store_navigation_quality: f64,
    pub bounce_rate: f64,
    pub buy_multiple_products: f64,
    pub search_quality: f64,
    pub view_product_to_buy: f64,
    pub refund: f64,
    pub product_rating: f64,
}

macro_rules! first_coeff {
    ($a:expr,$b:expr)=>{
        {
            if $a>0 {
                $a as f64
            }

            $b as f64
        }
    };
    ($a:expr,$b:expr,$c:expr)=>{
        {
            if $a>0 {
                $a as f64
            }

            if $b>0 {
                $b as f64
            }

            $c as f64
        }
    }
}

pub fn make_transitions(coef: &Coefficients) -> Vec<(Action, Vec<(Action, f64)>)> {
    vec![
        (
            Action::ViewIndex,
            vec![
                (Action::ViewIndexPromotions, coef.discover),
                (Action::SearchProduct, coef.discover * 0.8),
                (Action::SearchProduct, coef.search_for_product),
                (Action::ViewDeals, coef.discover * 0.7),
                (Action::Bounce, coef.bounce_rate),
                (Action::ViewOrders, coef.refund),
            ]
        ),
        (
            Action::ViewIndexPromotions,
            vec![
                (Action::ViewProduct, coef.discover),
                (Action::ViewIndex, coef.go_to_index),
            ]
        ),
        (
            Action::ViewDeals,
            vec![
                (Action::ViewProduct, coef.discover),
                (Action::ViewIndex, coef.go_to_index),
                (Action::Bounce, coef.bounce_rate),
            ]
        ),
        (
            Action::ViewOrders,
            vec![
                (Action::RefundProduct, coef.refund),
            ]
        ),
        (
            Action::SearchProduct,
            vec![
                (Action::ViewProduct, coef.discover),
                (Action::NotFound, coef.search_quality * 0.2),
                (Action::ViewIndex, coef.go_to_index),
                (Action::Bounce, coef.bounce_rate),
            ]
        ),
        (
            Action::NotFound,
            vec![
                (Action::SearchProduct, coef.discover),
                (Action::ViewIndex, coef.go_to_index),
                (Action::Bounce, coef.bounce_rate * 1.1),
            ]
        ),
        (
            Action::ViewProduct,
            vec![
                (Action::ViewIndex, coef.go_to_index),
                (Action::ViewRelatedProduct, coef.discover*0.5),
                (Action::AddProductToCart, coef.view_product_to_buy),
                (Action::Bounce, coef.bounce_rate),
            ]
        ),
        (
            Action::AddProductToCart,
            vec![
                (Action::SearchProduct, coef.buy_multiple_products),
                (Action::ViewIndex, coef.buy_multiple_products),
                (Action::ViewRelatedProduct, coef.discover),
                (Action::ViewCartAndPay, coef.view_product_to_buy),
                (Action::AbandonCart, coef.abandon_cart),
            ]
        ),
        (
            Action::ViewCartAndPay,
            vec![
                (Action::EnterCustomerInformation, coef.cart_completion),
                (Action::AbandonCart, coef.abandon_cart * 0.9),
            ]
        ),
        (
            Action::EnterCustomerInformation,
            vec![
                (Action::EnterShippingMethod, coef.cart_completion),
                (Action::AbandonCart, coef.abandon_cart * 0.8),
            ]
        ),
        (
            Action::EnterShippingMethod,
            vec![
                (Action::EnterPaymentMethod, coef.cart_completion),
                (Action::AbandonCart, coef.abandon_cart * 0.7),
            ]
        ),
        (
            Action::EnterPaymentMethod,
            vec![
                (Action::VerifyOrder, coef.cart_completion),
                (Action::AbandonCart, coef.abandon_cart * 0.6),
            ]
        ),
        (
            Action::VerifyOrder,
            vec![
                (Action::CompleteOrder, coef.cart_completion),
                (Action::AbandonCart, coef.abandon_cart * 0.5),
            ]
        ),
        (
            Action::CompleteOrder,
            vec![
                (Action::EndSession, 1.)
            ]
        ),
    ]
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;
    use std::rc::Rc;
    use chrono::Utc;
    use rand::distributions::WeightedIndex;
    use rand::thread_rng;
    // use crate::actions::{Actions, Probability, TransitionRules};
    use crate::store::products::{Preferences, Product, Products};
    use crate::store::scenario::{Action, make_transitions, run, Time};

    #[test]
    fn test() {
        let mut rng = thread_rng();
        let mut products = Products::try_new_from_csv("/Users/ravlio/projects/optiprism/optiprism/events-gen/src/store/data/products.csv", &mut rng.clone(), 1.2).unwrap();

        let preferences = Preferences {
            categories: None,
            subcategories: None,
            brand: None,
            author: None,
            size: None,
            color: None,
            max_price: None,
            min_price: None,
            min_rating: None,
            has_coupon: true,
        };

        let mut time = Time::new(Utc::now(), rng.clone());
        run(preferences, &mut time, &mut products, &mut rng.clone()).unwrap();
    }
}
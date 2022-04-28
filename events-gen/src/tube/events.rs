use std::borrow::Borrow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use chrono::{DateTime, Duration, TimeZone, Utc};
use lazy_static::lazy_static;
use rand::Rng;
use rand::rngs::ThreadRng;
use crate::tube::events::Event::{IndexPageViewed, ProductViewed};
use rand::distributions::{WeightedIndex};
use rand::prelude::*;

#[derive(Debug, Clone)]
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
    CartViewed,
    CouponApplied,
    CustomerInformation,
    ShippingMethodEntered,
    PaymentMethodEntered,
    OrderVerified,
    OrderCompleted,
    OrderRefunded,
}

pub enum Intention {
    JustBrowsing,
    BuyNewProduct,
    BuySpecificProduct,
    CheckForDeals,
    MakeRefund,
    RateProduct,
}

pub enum Source {
    SearchEngine,
    SocialMedia,
    AdNetwork,
    TypedIn,
}

pub enum Target {
    Index,
    Product,
}

#[derive(Clone)]
pub struct Product {
    id: u64,
}

struct ProductsProvider {
    promoted_products_weights: WeightedIndex<i32>,
    promoted_product_choices: Vec<Product>,
}

impl ProductsProvider {
    fn rand_promoted_product(&mut self, rng: &mut ThreadRng) -> &Product {
        &self.promoted_product_choices[self.promoted_products_weights.sample(rng)]
    }

    fn top_promoted_products(&self, n: usize) -> &[Product] {
        self.promoted_product_choices[..n].as_ref()
    }
}

type SessionRef = Rc<RefCell<Session>>;

pub struct Session {
    rng: ThreadRng,
    bought_products: HashMap<u64, ()>,
    viewed_products: HashMap<u64, ()>,
    events_store: EventStore,
    cur_time: DateTime<Utc>,
    intention: Intention,
    source: Source,
    target: Target,
    engagement_ratio: f64,
    churn_ratio: f64,
    slowness_ratio: f64,
    view_index_page_choices: WeightedIndex<i32>,
    products: ProductsProvider,
}

#[derive(Debug, Clone)]
pub enum Choice {
    Search,
    ViewIndexPage,
    ViewIndexPagePromotion,
    ViewDeals,
    ViewProduct,
}

fn invoke(session_ref: SessionRef, choice: &Choice) -> Option<Choice> {
    match choice {
        Choice::Search => search(session_ref),
        Choice::ViewIndexPage => view_index_page(session_ref),
        Choice::ViewIndexPagePromotion => view_index_page_promotion(session_ref),
        Choice::ViewDeals => view_deals(session_ref),
        Choice::ViewProduct => view_product(session_ref),
    }
}

pub fn view_index_page(session_ref: Rc<RefCell<Session>>) -> Option<Choice> {
    let mut session = session_ref.borrow_mut();
    session.events_store.push(EventRecord::new(IndexPageViewed));
    session.wait_between(Duration::seconds(10), Duration::seconds(30));
    let choices = [&Choice::Search, &Choice::ViewIndexPagePromotion];
    let choice = choices[session.view_index_page_choices_sample()];
    invoke(session_ref.clone(), choice)
}

pub fn search(session_ref: SessionRef) -> Option<Choice> {
    None
}

pub fn view_index_page_promotion(session_ref: SessionRef) -> Option<Choice> {
    let mut session = session_ref.borrow_mut();
    session.wait_between(Duration::seconds(10), Duration::seconds(30));

    for product in session_ref.borrow_mut().products.top_promoted_products(10).iter() {
        let bought_products = session.bought_products.len();
        if session.viewed_products.contains_key(&product.id) {
            continue;
        }

        if session.rng.gen_ratio(10, 100) {
            if invoke(session_ref.clone(), &Choice::ViewProduct).is_none() || session.bought_products.len() - bought_products > 0 {
                return None;
            }
        }
    }

    None
}

pub fn view_deals(session_ref: SessionRef) -> Option<Choice> {
    None
}

pub fn view_product(session_ref: SessionRef) -> Option<Choice> {
    let mut session = session_ref.borrow_mut();
    session.events_store.push(EventRecord::new(ProductViewed));
    None
}

impl Session {
    fn view_index_page_choices_sample(&mut self) -> usize {
        self.view_index_page_choices.sample(&mut self.rng)
    }

    fn wait_between(&mut self, from: Duration, to: Duration) {
        let wait = Duration::seconds(self.rng.gen_range(from.num_seconds()..=to.num_seconds()));
        self.cur_time = self.cur_time.clone() + wait;
    }
}


#[derive(Eq, PartialEq, Hash, Clone, Debug)]
enum EventProperty {}

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
enum UserProperty {}

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
enum Value {}

#[derive(Debug, Clone)]
pub struct EventRecord {
    event: Event,
    event_properties: HashMap<EventProperty, Value>,
    user_properties: HashMap<UserProperty, Value>,
}

impl EventRecord {
    pub fn new(e: Event) -> EventRecord {
        EventRecord {
            event: e,
            event_properties: Default::default(),
            user_properties: Default::default(),
        }
    }
}

pub struct EventStore {
    events: Vec<EventRecord>,
    // fixed props
    event_properties: HashMap<EventProperty, Value>,
    user_properties: HashMap<UserProperty, Value>,
}

impl EventStore {
    pub fn push(&mut self, e: EventRecord) {
        let merged_event = EventRecord {
            event: e.event.clone(),
            event_properties: {
                let mut ep = self.event_properties.clone();
                ep.extend(e.event_properties.clone());
                ep
            },
            user_properties: {
                let mut up = self.user_properties.clone();
                up.extend(e.user_properties.clone());
                up
            },
        };

        println!("{:?}", e);
        self.events.push(merged_event)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use chrono::Utc;
    use rand::distributions::WeightedIndex;
    use rand::rngs::ThreadRng;
    use crate::tube::events::{Choice, EventStore, Intention, invoke, Product, ProductsProvider, Session, Source, Target, view_index_page};

    #[test]
    fn test() {
        let data = Rc::new(RefCell::new(Vec::new()));
        data.borrow_mut().push(5);
        println!("{:?}", data.borrow().last());

        let mut es = EventStore {
            events: vec![],
            event_properties: Default::default(),
            user_properties: Default::default(),
        };

        let mut prods = ProductsProvider {
            promoted_products_weights: WeightedIndex::new(&[2, 1]).unwrap(),
            promoted_product_choices: vec![
                Product {
                    id: 1
                },
                Product {
                    id: 2
                },
                Product {
                    id: 3
                },
            ],
        };

        let mut session_ref = Rc::new(RefCell::new(Session {
            rng: Default::default(),
            bought_products: Default::default(),
            viewed_products: Default::default(),
            events_store: es,
            cur_time: Utc::now(),
            intention: Intention::JustBrowsing,
            source: Source::SearchEngine,
            target: Target::Index,
            engagement_ratio: 0.2,
            churn_ratio: 0.1,
            slowness_ratio: 0.0,
            view_index_page_choices: {
                let weights = [2, 1, 1];
                WeightedIndex::new(&weights).unwrap()
            },
            products: prods,
        }));

        let mut rng = ThreadRng::default();
        invoke(session_ref, &Choice::ViewIndexPage);
    }
}
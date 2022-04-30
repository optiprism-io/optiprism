use std::collections::HashMap;
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

#[derive(Debug, Clone)]
pub enum Intention {
    JustBrowsing,
    BuyNewProduct,
    BuySpecificProduct,
    CheckForDeals,
    MakeRefund,
    RateProduct,
}

#[derive(Debug, Clone)]
pub enum Source {
    SearchEngine,
    SocialMedia,
    AdNetwork,
    TypedIn,
}

#[derive(Debug, Clone)]
pub enum Target {
    Index,
    Product,
}

#[derive(Debug, Clone)]
pub struct Product {
    id: u64,
}

#[derive(Debug, Clone)]
struct ProductsProvider {
    promoted_products_weights: WeightedIndex<i32>,
    promoted_product_choices: Vec<Product>,
}

impl ProductsProvider {
    fn rand_promoted_product(&mut self, rng: &mut ThreadRng) -> Product {
        self.promoted_product_choices[self.promoted_products_weights.sample(rng)].clone()
    }

    fn top_promoted_products(&self, n: usize) -> Vec<Product> {
        self.promoted_product_choices[..n].to_vec()
    }
}

struct ViewIndexPage {}

impl ActionTr for ViewIndexPage {
    fn evaluate(&mut self, model: &ActionProbabilities) -> Result<(), String> {
        self.events_store.push(EventRecord::new(IndexPageViewed));
        self.wait_between(Duration::seconds(10), Duration::seconds(30));
        Ok(())
    }

    fn name() -> &str {
        "view_index_page"
    }
}

struct ViewIndexPagePromotion {}

impl ActionTr for ViewIndexPagePromotion {
    fn evaluate(&mut self, session: &mut Session) -> Result<(), String> {
        session.wait_between(Duration::seconds(10), Duration::seconds(30));

        for product in session.products.top_promoted_products(3).iter() {
            let bought_products = session.bought_products.len();
            if session.viewed_products.contains_key(&product.id) {
                continue;
            }

            if session.transition_chance(&Action::ViewIndexPagePromotion) {
                session.invoke(&Action::ViewProduct);
                if session.bought_products.len() - bought_products > session.max_products_to_buy {
                    return;
                }
            }
        }

        Ok(())
    }

    fn name() -> &str {
        "view_index_page_promotion"
    }
}

#[derive(Debug, Clone)]
pub struct Session {
    rng: ThreadRng,
    bought_products: HashMap<u64, ()>,
    viewed_products: HashMap<u64, ()>,
    max_products_to_buy: usize,
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
    action_probabilities: HashMap<String, Vec<TransitionTo>>,
}

#[derive(Debug, Clone)]
enum Page {
    Index,
    Product,
    Deals,
}

pub enum PageSection {
    IndexFeatured,
}

#[derive(Debug, Clone, Hash, PartialEq)]
enum Action {
    Search,
    OpenPage(Page),
    ScrollTo(PageSection),
}

pub enum Probability {
    Uniform(f64),
    Steps(Vec<f64>),
}

pub struct TransitionTo {
    to: Box<dyn ActionTr>,
    probability: Probability,
    limit: Option<usize>,
}

impl TransitionTo {
    pub fn new(to: Box<dyn ActionTr>, probability: Probability, limit: Option<usize>) -> Self {
        Self {
            to,
            probability,
            limit,
        }
    }
}

pub struct Transitions {
    transitions: Vec<TransitionTo>,
}

trait ActionTr<> {
    fn evaluate(&mut self, model: &ActionProbabilities) -> Result<(), String>;
    fn name() -> &str;
}

pub struct ActionProbabilities {
    transitions: HashMap<String, Vec<TransitionTo>>,
}

impl ActionProbabilities {
    pub fn new() -> Self {
        ActionProbabilities { transitions: Default::default() }
    }
}

impl ActionProbabilities {
    pub fn transition(&mut self, from: Action, to: Action, probability: Probability, limit: Option<usize>) {
        let transition = TransitionTo::new(to, probability, limit);
        if Some(t) = self.transitions.get_mut(&from) {
            t.push(transition);
        } else {
            self.transitions.insert(from, vec![transition]);
        }
    }
}

impl Transition {
    pub fn new(from: Action, to: Action, probability: Probability, limit: Option<usize>) -> Self {
        Self {
            from,
            to,
            probability,
            limit,
        }
    }
}

fn view_index_page_promotion(session: &mut Session) {
    session.wait_between(Duration::seconds(10), Duration::seconds(30));

    for product in session.products.top_promoted_products(3).iter() {
        let bought_products = session.bought_products.len();
        if session.viewed_products.contains_key(&product.id) {
            continue;
        }

        if session.transition_chance(&Action::ViewIndexPagePromotion) {
            session.invoke(&Action::ViewProduct);
            if session.bought_products.len() - bought_products > session.max_products_to_buy {
                return;
            }
        }
    }
}

impl Session {
    pub fn transition_chance(&self, from: &Action, to: &Action) -> bool {
        session.rng.gen::<f64>()
    }

    fn invoke(&mut self, choice: &Action) {
        match choice {
            Action::Search => self.search(),
            Action::ViewIndexPage => self.view_index_page(),
            Action::ViewIndexPagePromotion => view_index_page_promotion(self),
            Action::ViewDeals => self.view_deals(),
            Action::ViewProduct => self.view_product(),
        }
    }

    fn wait_between(&mut self, from: Duration, to: Duration) {
        let wait = Duration::seconds(self.rng.gen_range(from.num_seconds()..=to.num_seconds()));
        self.cur_time = self.cur_time.clone() + wait;
    }

    fn check_action_probability(&mut self, from_action: &str, to_action: &str) -> bool {
        true
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

#[derive(Debug, Clone)]
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
    use chrono::Utc;
    use rand::distributions::WeightedIndex;
    use crate::tube::events::{Action, EventStore, Intention, ActionProbabilities, Page, PageSection, Probability, Product, ProductsProvider, Session, Source, Target, Transition};

    #[test]
    fn test() {
        let es = EventStore {
            events: vec![],
            event_properties: Default::default(),
            user_properties: Default::default(),
        };

        let prods = ProductsProvider {
            promoted_products_weights: WeightedIndex::new(&[2, 1, 1]).unwrap(),
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

        let mut sess = Session {
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
                let weights = [2, 1];
                WeightedIndex::new(&weights).unwrap()
            },
            products: prods,
        };

        sess.view_index_page();

        let prob = Transition::new(
            Action::OpenPage(Page::Index),
            Action::OpenPage(Page::Index),
        );

        let mut model = ActionProbabilities::new();
        // Index page
        model.transition(
            Action::OpenPage(Page::Index),
            Action::ScrollTo(PageSection::IndexFeatured),
            Probability::Uniform(0.3),
            None,
        );
    }
}
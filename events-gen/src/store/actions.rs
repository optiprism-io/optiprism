use std::fmt;
use chrono::Duration;
use crate::session::{Action, ActionTransition, EventRecord, Session};
use crate::store::data::{Product, Products};
use crate::store::input::Input;
use crate::store::props::{EventProperties, UserProperties};
use crate::store::state::State;
use crate::error::{Error, Result};

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

impl fmt::Display for Event {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
enum ActionName {
    ViewIndexPage,
    ViewIndexPagePromotion,
    ViewProduct,
}

struct ViewIndexPage {}

impl ViewIndexPage {
    pub fn new() -> Self {
        Self {}
    }
}

impl Action<Products, State, Input, EventProperties, UserProperties, ActionName> for ViewIndexPage {
    fn evaluate(&self, session: &mut Session<Products, State, Input, EventProperties, UserProperties, ActionName>) -> Result<()> {
        session.write_event(Event::IndexPageViewed.to_string(), None)?;
        session.wait_between(Duration::seconds(10), Duration::seconds(30));
        session.try_auto_transit(ActionName::ViewIndexPage)?;

        Ok(())
    }

    fn dyn_name(&self) -> ActionName {
        ActionName::ViewIndexPage
    }
}

struct ViewIndexPagePromotion {}

impl Action<Products, State, Input, EventProperties, UserProperties, ActionName> for ViewIndexPagePromotion {
    fn evaluate(&self, session: &mut Session<Products, State, Input, EventProperties, UserProperties, ActionName>) -> Result<()> {
        session.wait_between(Duration::seconds(10), Duration::seconds(30));
        session.try_auto_transit(ActionName::ViewIndexPagePromotion)?;

        for product in session.data_provider.top_promoted_products(3).iter() {
            if session.data_state.products_viewed.contains_key(&product.id) {
                continue;
            }

            session.try_transit(ActionTransition::new(ActionName::ViewIndexPagePromotion, ActionName::ViewProduct))?;
        }

        Ok(())
    }

    fn dyn_name(&self) -> ActionName {
        ActionName::ViewIndexPagePromotion
    }
}

struct ViewProduct {}

impl Action<Products, State, Input, EventProperties, UserProperties, ActionName> for ViewProduct {
    fn evaluate(&self, session: &mut Session<Products, State, Input, EventProperties, UserProperties, ActionName>) -> Result<()> {
        session.wait_between(Duration::seconds(10), Duration::seconds(30));
        session.write_event(Event::ProductViewed.to_string(), None)?;
        session.try_auto_transit(ActionName::ViewProduct)?;

        Ok(())
    }

    fn dyn_name(&self) -> ActionName {
        ActionName::ViewProduct
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use chrono::Utc;
    use rand::distributions::WeightedIndex;
    use crate::session::{Action, ActionTransition, Probability, Session, TransitionRule};
    use crate::store::actions::{ActionName, ViewIndexPage, ViewIndexPagePromotion, ViewProduct};
    use crate::store::data::{Product, Products};
    use crate::store::input::Input;
    use crate::store::output::PrintOutput;
    use crate::store::props::{EventProperties, UserProperties};
    use crate::store::state::State;

    #[test]
    fn test() {
        let prods = Products {
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

        let state = State::new();

        let input = Input {
            max_product_to_buy: 2,
        };

        let output = Box::new(PrintOutput {});
        let event_props = EventProperties {};
        let user_props = UserProperties {};

        let actions: Vec<Rc<dyn Action<Products, State, Input, EventProperties, UserProperties, ActionName>>> = vec![
            Rc::new(ViewIndexPage {}),
            Rc::new(ViewIndexPagePromotion {}),
            Rc::new(ViewProduct {}),
        ];

        let probs = vec![
            (
                ActionTransition::new(ActionName::ViewIndexPage, ActionName::ViewIndexPagePromotion),
                TransitionRule::new(Probability::Uniform(0.5), None)
            ),
            (
                ActionTransition::new(ActionName::ViewIndexPagePromotion, ActionName::ViewProduct),
                TransitionRule::new(Probability::Uniform(0.5), None)
            ),
        ];

        let mut session = Session::new(
            Utc::now(),
            prods,
            state,
            input,
            output,
            actions,
            probs,
            event_props,
            user_props,
        );

        session.run(ActionName::ViewIndexPage).unwrap();
    }
}
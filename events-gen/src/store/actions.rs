use std::fmt;
use chrono::Duration;
use crate::session::{Action, EventRecord, Session};
use crate::store::data::{Data, Product, Products};
use crate::store::input::Input;
use crate::store::state::State;
use crate::error::{Error, Result};
use std::string::ToString;
use strum_macros::Display;

#[derive(Debug, Clone, Display)]
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

#[derive(Clone, Debug, Hash, Eq, PartialEq, Display)]
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

impl Action<Data> for ViewIndexPage {
    fn evaluate(&self, session: &mut Session<Data>) -> Result<()> {
        // TODO make Event as generic
        session.write_event(Event::IndexPageViewed, None)?;
        session.wait_between(Duration::seconds(10), Duration::seconds(30));
        session.try_auto_transit(ActionName::ViewIndexPage)?;

        Ok(())
    }

    fn name(&self) -> String {
        ActionName::ViewIndexPage.to_string()
    }
}

struct ViewIndexPagePromotion {}

impl Action<Data> for ViewIndexPagePromotion {
    fn evaluate(&self, session: &mut Session<Data>) -> Result<()> {
        session.wait_between(Duration::seconds(10), Duration::seconds(30));
        session.try_auto_transit(ActionName::ViewIndexPagePromotion)?;

        for product in session.data.products.top_promoted_products(3).iter() {
            if session.data.state.products_viewed.contains_key(&product.id) {
                continue;
            }

            session.try_transit(ActionName::ViewIndexPagePromotion, ActionName::ViewProduct)?;
        }

        Ok(())
    }

    fn name(&self) -> String {
        ActionName::ViewIndexPagePromotion.to_string()
    }
}

struct ViewProduct {}

impl Action<Data> for ViewProduct {
    fn evaluate(&self, session: &mut Session<Data>) -> Result<()> {
        session.wait_between(Duration::seconds(10), Duration::seconds(30));
        session.write_event(Event::ProductViewed, None)?;
        session.try_auto_transit(ActionName::ViewProduct)?;

        Ok(())
    }

    fn name(&self) -> String {
        ActionName::ViewProduct.to_string()
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use chrono::Utc;
    use rand::distributions::WeightedIndex;
    use crate::session::{Action, Probability, Session, TransitionRules};
    use crate::store::actions::{ActionName, ViewIndexPage, ViewIndexPagePromotion, ViewProduct};
    use crate::store::data::{Data, Product, Products};
    use crate::store::input::Input;
    use crate::store::output::PrintOutput;
    use crate::store::state::State;

    #[test]
    fn test() {
        let products = Products {
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

        let data = Data {
            products,
            input,
            state,
        };

        let output = Box::new(PrintOutput {});

        let actions: Vec<Rc<dyn Action<Data>>> = vec![
            Rc::new(ViewIndexPage {}),
            Rc::new(ViewIndexPagePromotion {}),
            Rc::new(ViewProduct {}),
        ];

        let transitions = vec![
            (
                (ActionName::ViewIndexPage, ActionName::ViewIndexPagePromotion),
                TransitionRules::new(Probability::Uniform(0.5), None)
            ),
            (
                (ActionName::ViewIndexPagePromotion, ActionName::ViewProduct),
                TransitionRules::new(Probability::Uniform(0.5), None)
            ),
        ];

        let mut session = Session::new(
            Utc::now(),
            data,
            output,
            actions,
            transitions,
            None,
        );

        session.run(ActionName::ViewIndexPage).unwrap();
    }
}
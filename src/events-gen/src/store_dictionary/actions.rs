use strum_macros::Display;

use crate::store_dictionary::events::Event;

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
}

impl Action {
    pub fn to_event(self) -> Option<Event> {
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

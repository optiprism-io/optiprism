use crate::store::actions::Action;
use crate::store::coefficients::Coefficients;
use crate::store::scenario::Transition;

pub fn make_transitions(coef: &Coefficients) -> Vec<Transition> {
    vec![
        (Action::ViewIndex, vec![
            (Action::ViewIndexPromotions, coef.discover, 3),
            (Action::SearchProduct, coef.discover * 0.8, 15),
            (Action::SearchProduct, coef.search_for_product, 6),
            (Action::ViewDeals, coef.discover * 0.7, 8),
            (Action::Bounce, coef.bounce_rate, 5),
            (Action::ViewOrders, coef.refund, 7),
            (Action::Register, coef.register * 0.2, 60),
            (Action::Login, coef.login * 0.2, 20),
        ]),
        (Action::ViewIndexPromotions, vec![
            (Action::ViewProduct, coef.discover, 10),
            (Action::ViewIndex, coef.go_to_index, 0),
            (Action::Register, coef.register * 0.2, 60),
            (Action::Login, coef.login * 0.2, 20),
        ]),
        (Action::ViewDeals, vec![
            (Action::ViewProduct, coef.discover, 10),
            (Action::ViewIndex, coef.go_to_index, 5),
            (Action::Bounce, coef.bounce_rate, 5),
            (Action::Register, coef.register * 0.2, 60),
            (Action::Login, coef.login * 0.2, 20),
        ]),
        (Action::ViewOrders, vec![
            (Action::RefundProduct, coef.refund, 10),
            (Action::Register, coef.register, 60),
            (Action::Login, coef.login, 20),
        ]),
        (Action::RefundProduct, vec![
            (Action::EndSession, 1., 0),
            (Action::Register, coef.register, 60),
            (Action::Login, coef.login, 20),
        ]),
        (Action::SearchProduct, vec![
            (Action::ViewProduct, coef.discover, 15),
            (Action::NotFound, coef.search_quality * 0.2, 4),
            (Action::ViewIndex, coef.go_to_index, 5),
            (Action::Register, coef.register * 0.2, 60),
            (Action::Login, coef.login * 0.2, 20),
            (Action::Bounce, coef.bounce_rate, 5),
        ]),
        (Action::NotFound, vec![
            (Action::SearchProduct, coef.discover, 0),
            (Action::ViewIndex, coef.go_to_index, 0),
            (Action::Bounce, coef.bounce_rate * 1.1, 0),
        ]),
        (Action::ViewProduct, vec![
            (Action::ViewIndex, coef.go_to_index, 5),
            (Action::ViewRelatedProduct, coef.discover * 0.5, 20),
            (Action::AddProductToCart, coef.view_product_to_buy, 30),
            (Action::Register, coef.register * 0.2, 60),
            (Action::Login, coef.login * 0.2, 20),
            (Action::Bounce, coef.bounce_rate, 5),
        ]),
        (Action::ViewRelatedProduct, vec![
            (Action::ViewIndex, coef.go_to_index, 5),
            (Action::ViewRelatedProduct, coef.discover * 0.2, 20),
            (Action::AddProductToCart, coef.view_product_to_buy * 0.9, 30),
            (Action::Register, coef.register * 0.2, 60),
            (Action::Login, coef.login * 0.2, 20),
            (Action::Bounce, coef.bounce_rate, 5),
        ]),
        (Action::AddProductToCart, vec![
            (Action::SearchProduct, coef.buy_multiple_products, 6),
            (Action::ViewIndex, coef.buy_multiple_products, 5),
            (Action::ViewRelatedProduct, coef.discover, 10),
            (Action::ViewCart, coef.view_cart, 5),
            (Action::AbandonCart, coef.abandon_cart, 5),
            (Action::Register, coef.register * 0.7, 60),
            (Action::Login, coef.login * 0.7, 20),
        ]),
        (Action::ViewCart, vec![
            (Action::EnterCustomerInformation, coef.view_cart, 5),
            (Action::AbandonCart, coef.abandon_cart * 0.9, 5),
            (Action::Register, coef.register * 10., 60),
            (Action::Login, coef.login * 10., 20),
        ]),
        (Action::EnterCustomerInformation, vec![
            (Action::EnterShippingMethod, coef.cart_completion, 20),
            (Action::AbandonCart, coef.abandon_cart * 0.8, 5),
        ]),
        (Action::EnterShippingMethod, vec![
            (Action::EnterPaymentMethod, coef.cart_completion, 15),
            (Action::AbandonCart, coef.abandon_cart * 0.7, 5),
        ]),
        (Action::EnterPaymentMethod, vec![
            (Action::VerifyOrder, coef.cart_completion, 20),
            (Action::AbandonCart, coef.abandon_cart * 0.6, 5),
        ]),
        (Action::VerifyOrder, vec![
            (Action::CompleteOrder, coef.cart_completion, 10),
            (Action::AbandonCart, coef.abandon_cart * 0.5, 5),
        ]),
        (Action::CompleteOrder, vec![(Action::EndSession, 1., 10)]),
        (Action::Register, vec![(Action::Register, 1., 20)]),
        (Action::Login, vec![(Action::Login, 1., 20)]),
    ]
}

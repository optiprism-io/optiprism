use enum_iterator::Sequence;
use strum_macros::Display;

use crate::store::intention::Intention;

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
    pub view_cart: f64,
    pub refund: f64,
    pub product_rating: f64,
    pub register: f64,
    pub login: f64,
    pub logout: f64,
}

pub fn make_coefficients(intention: &Intention, src: &Option<AdSource>) -> Coefficients {
    let c = match intention {
        Intention::BuyCertainProduct(_) => Coefficients {
            abandon_cart: 0.01,
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
            view_cart: 0.5,
            refund: 0.0,
            product_rating: 0.9,
            register: 0.8,
            login: 0.8,
            logout: 0.1,
        },
        Intention::BuyAnyProduct => Coefficients {
            abandon_cart: 0.01,
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
            view_cart: 0.5,
            refund: 0.0,
            product_rating: 0.9,
            register: 0.8,
            login: 0.8,
            logout: 0.1,
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
            view_cart: 0.5,
            refund: 0.0,
            product_rating: 0.9,
            register: 0.2,
            login: 0.2,
            logout: 0.1,
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
            view_cart: 0.,
            refund: 1.0,
            product_rating: 0.,
            register: 0.0,
            login: 1.,
            logout: 0.0,
        },
    };

    if let Some(src) = &src {
        multiply(c, &utm_coefficients(src))
    } else {
        c
    }
}
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, Display)]
pub enum AdSource {
    #[strum(serialize = "AdWords")]
    AdWords,
    #[strum(serialize = "Facebook Text")]
    FacebookText,
    #[strum(serialize = "Facebook Banner")]
    FacebookBanner,
}
fn utm_coefficients(company: &AdSource) -> Coefficients {
    match company {
        AdSource::AdWords => Coefficients {
            abandon_cart: 1.,
            cart_completion: 1.2,
            discover: 1.2,
            search_for_product: 1.2,
            go_to_index: 1.,
            store_navigation_quality: 1.,
            bounce_rate: 1.,
            global_bounce_rate: 1.,
            buy_multiple_products: 1.,
            search_quality: 1.,
            view_product_to_buy: 1.3,
            view_cart: 1.,
            refund: 1.,
            product_rating: 1.,
            register: 1.,
            login: 1.,
            logout: 1.,
        },
        AdSource::FacebookText => Coefficients {
            abandon_cart: 1.,
            cart_completion: 1.1,
            discover: 1.1,
            search_for_product: 1.1,
            go_to_index: 1.,
            store_navigation_quality: 1.,
            bounce_rate: 1.,
            global_bounce_rate: 1.,
            buy_multiple_products: 1.,
            search_quality: 1.,
            view_product_to_buy: 1.1,
            view_cart: 1.,
            refund: 1.,
            product_rating: 1.,
            register: 1.,
            login: 1.,
            logout: 1.,
        },
        AdSource::FacebookBanner => Coefficients {
            abandon_cart: 1.1,
            cart_completion: 0.9,
            discover: 1.2,
            search_for_product: 0.9,
            go_to_index: 1.,
            store_navigation_quality: 1.,
            bounce_rate: 1.,
            global_bounce_rate: 1.2,
            buy_multiple_products: 1.,
            search_quality: 1.,
            view_product_to_buy: 0.9,
            view_cart: 0.9,
            refund: 1.,
            product_rating: 1.,
            register: 0.7,
            login: 1.,
            logout: 1.,
        },
    }
}

fn multiply(c: Coefficients, mul: &Coefficients) -> Coefficients {
    Coefficients {
        abandon_cart: c.abandon_cart * mul.abandon_cart,
        cart_completion: c.cart_completion * mul.cart_completion,
        discover: c.discover * mul.discover,
        search_for_product: c.search_for_product * mul.search_for_product,
        go_to_index: c.go_to_index * mul.go_to_index,
        store_navigation_quality: c.store_navigation_quality * mul.store_navigation_quality,
        bounce_rate: c.bounce_rate * mul.bounce_rate,
        global_bounce_rate: c.global_bounce_rate * mul.global_bounce_rate,
        buy_multiple_products: c.buy_multiple_products * mul.buy_multiple_products,
        search_quality: c.search_quality * mul.search_quality,
        view_product_to_buy: c.view_product_to_buy * mul.view_product_to_buy,
        view_cart: c.view_cart * mul.view_cart,
        refund: c.refund * mul.refund,
        product_rating: c.product_rating * mul.product_rating,
        register: c.register * mul.register,
        login: c.login * mul.login,
        logout: c.logout * mul.logout,
    }
}

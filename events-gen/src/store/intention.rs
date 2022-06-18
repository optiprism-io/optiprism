use crate::store::products::{Product, ProductProvider};
use crate::store::scenario::State;
use rand::prelude::*;
use rand::rngs::ThreadRng;

#[derive(Clone, Copy, Debug)]
pub enum Intention<'a> {
    BuyCertainProduct(&'a Product),
    BuyAnyProduct,
    JustBrowse,
    MakeRefund(&'a Product),
}

pub fn select_intention<'a>(
    state: &State,
    products: &'a ProductProvider,
    rng: &mut ThreadRng,
) -> Intention<'a> {
    if state.session_id > 0 && !state.products_bought.is_empty() && rng.gen::<f64>() < 0.1 {
        for (id, _) in state.products_bought.iter() {
            if rng.gen::<f64>() < 0.5 && !state.products_refunded.contains_key(id) {
                return Intention::MakeRefund(&products.products[*id - 1]);
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

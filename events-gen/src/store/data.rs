use std::collections::HashMap;
use rand::distributions::WeightedIndex;
use rand::Rng;
use rand::rngs::ThreadRng;
use rand::prelude::*;

#[derive(Debug, Clone)]
pub struct Product {
    pub id: u64,
}

#[derive(Debug, Clone)]
pub struct Products {
    pub promoted_products_weights: WeightedIndex<i32>,
    pub promoted_product_choices: Vec<Product>,
}

impl Products {
/*    pub fn new() -> Self {
        Self {
            promoted_products_weights: WeightedIndex,
            promoted_product_choices: vec![]
        }
    }*/
    pub fn rand_promoted_product(&mut self, rng: &mut ThreadRng) -> Product {
        self.promoted_product_choices[self.promoted_products_weights.sample(rng)].clone()
    }

    pub fn top_promoted_products(&self, n: usize) -> Vec<Product> {
        self.promoted_product_choices[..n].to_vec()
    }
}
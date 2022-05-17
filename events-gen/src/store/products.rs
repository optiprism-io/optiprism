use std::collections::{HashMap, HashSet};
use std::env;
use std::path::Path;
use rand::distributions::WeightedIndex;
use rand::rngs::ThreadRng;
use rand::prelude::*;
use serde::Deserialize;
use crate::error::{Result, Error};
use rand::seq::SliceRandom;
use crate::probability;

pub struct Preferences {
    pub categories: Option<Vec<String>>,
    pub subcategories: Option<Vec<String>>,
    pub brand: Option<Vec<String>>,
    pub author: Option<Vec<String>>,
    pub size: Option<Vec<String>>,
    pub color: Option<Vec<String>>,
    pub max_price: Option<f64>,
    pub min_price: Option<f64>,
    pub min_rating: Option<f64>,
    pub has_coupon: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Product {
    pub id: usize,
    pub name: String,
    pub category: String,
    pub subcategory: Option<String>,
    pub brand: Option<String>,
    pub author: Option<String>,
    pub size: Option<String>,
    pub color: Option<String>,
    pub price: f64,
    pub discount_price: Option<f64>,
    pub margin: f64,
    // affects rating
    pub satisfaction_ratio: f64,
    pub rating_count: usize,
    pub rating_sum: f64,
}


impl Product {
    pub fn rating(&self) -> f64 {
        self.rating_sum / self.rating_count as f64
    }

    pub fn final_price(&self, has_coupon: bool) -> f64 {
        match (has_coupon, self.discount_price) {
            (true, Some(p)) => p * self.margin,
            _ => self.price * self.margin
        }
    }

    pub fn calc_buy_coefficient(&self, preferences: &Preferences, rating_weights: &[f64]) -> f64 {
        let price = self.final_price(preferences.has_coupon);
        let mut coefficient = rating_weights[(self.rating() * 10.) as usize];

        if self.discount_price.is_some() {
            if preferences.has_coupon {
                coefficient *= 1.2
            } else {
                coefficient *= 1.1
            }
        }

        if let Some(v) = &preferences.categories {
            if v.contains(&self.category) {
                coefficient *= 1.1;
            }
        }

        match (&preferences.subcategories, &self.subcategory) {
            (Some(a), Some(b)) => {
                if a.contains(&b) {
                    coefficient *= 1.05;
                }
            }
            _ => {}
        }

        match (&preferences.brand, &self.brand) {
            (Some(a), Some(b)) => {
                if a.contains(&b) {
                    coefficient *= 1.1;
                }
            }
            _ => {}
        }

        match (&preferences.author, &self.author) {
            (Some(a), Some(b)) => {
                if a.contains(&b) {
                    coefficient *= 1.1;
                }
            }
            _ => {}
        }

        match (&preferences.size, &self.size) {
            (Some(a), Some(b)) => {
                if a.contains(&b) {
                    coefficient *= 1.1;
                }
            }
            _ => {}
        }

        match (&preferences.color, &self.color) {
            (Some(a), Some(b)) => {
                if a.contains(&b) {
                    coefficient *= 1.05;
                }
            }
            _ => {}
        }

        match &preferences.max_price {
            Some(p) => {
                if *p > price {
                    coefficient *= 1.1;
                }
            }
            _ => {}
        }

        match &preferences.min_price {
            Some(p) => {
                if *p < price {
                    coefficient *= 1.05;
                }
            }
            _ => {}
        }

        match &preferences.min_rating {
            Some(r) => {
                if *r < self.rating() {
                    coefficient *= 1.05;
                }
            }
            _ => {}
        }

        coefficient
    }
}

#[derive(Debug, Clone)]
pub struct Products {
    pub products: Vec<Product>,
    product_weights: WeightedIndex<f64>,
    pub promoted_products: Vec<usize>,
    promoted_product_weights: WeightedIndex<f64>,
    pub deal_products: Vec<usize>,
    deal_product_weights: WeightedIndex<f64>,
    pub categories: Vec<String>,
    pub category_weights: WeightedIndex<f64>,
    rating_weights: Vec<f64>,
    margin: f64,

}

impl Products {
    pub fn try_new_from_csv<P: AsRef<Path>>(path: P, rng: &mut ThreadRng, margin: f64) -> Result<Self> {
        let mut rdr = csv::Reader::from_path(path)?;
        let mut products = rdr.deserialize().collect::<csv::Result<Vec<Product>>>()?;
        products.shuffle(rng);
        let product_weights = WeightedIndex::new(
            probability::calc_cubic_spline(products.len(), vec![1., 0.5, 0.3, 0.1])?)
            .map_err(|err| Error::External(err.to_string()))?;

        let promoted_products = products[0..5].iter().map(|p| p.id).collect();
        let promoted_product_weights = WeightedIndex::new(&[1., 0.3, 0.2, 0.1, 0.1]).unwrap();

        let deal_products = products.iter().filter(|p| p.discount_price.is_some()).map(|p| p.id).collect();
        let deal_product_weights = WeightedIndex::new(&[1., 0.3, 0.2, 0.1, 0.1]).unwrap();

        let mut categories = products
            .iter()
            .map(|p| p.category.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<String>>();
        categories.shuffle(rng);
        let category_weights = WeightedIndex::new(
            probability::calc_cubic_spline(categories.len(), vec![1., 0.5, 0.3, 0.1])?)
            .map_err(|err| Error::External(err.to_string()))?;


        // make rating weights from 0 to 5 with 10 bins for each int value
        let rating_weights = probability::calc_cubic_spline(50, vec![0.01, 0.01, 0.1, 0.7, 1.])?;
        Ok(Self {
            products,
            product_weights,
            promoted_products,
            promoted_product_weights,
            deal_products,
            deal_product_weights,
            categories,
            category_weights,
            rating_weights,
            margin,
        })
    }

    pub fn deal_product_sample(&self, rng: &mut ThreadRng) -> &Product {
        &self.products[self.deal_products[self.deal_product_weights.sample(rng)]]
    }

    pub fn promoted_product_sample(&self, rng: &mut ThreadRng) -> &Product {
        &self.products[self.promoted_products[self.promoted_product_weights.sample(rng)]]
    }

    pub fn get_product_by_id(&self, id: usize) -> &Product {
        &self.products[id]
    }
    pub fn rate_product(&mut self, id: usize, rating: f64) {
        let product = &mut self.products[id];
        product.rating_count += 1;
        product.rating_sum += rating;
    }

    /*pub fn calc_buy_factor_for_product(&self, id: usize, preferences: &Preferences) -> f64 {
        let product = &self.products[id];
        let price = product.final_price(preferences.has_coupon);
        let mut prob = self.rating_weights[(product.rating() * 10.0) as usize];

        if product.discount_price.is_some() {
            if preferences.has_coupon {
                prob *= 1.2
            } else {
                prob *= 1.1
            }
        }

        match &preferences.categories {
            Some(v) => {
                if v.iter().find(|c| **c == product.category) {
                    prob *= 1.1;
                }
            }
            None => {}
        }

        match (&preferences.subcategories, &product.subcategory) {
            (Some(a), Some(b)) => {
                if a.iter().find(|c| **c == b) {
                    prob *= 1.05;
                }
            }
            _ => {}
        }

        match (&preferences.brand, &product.brand) {
            (Some(a), Some(b)) => {
                if a.iter().find(|c| **c == b) {
                    prob *= 1.1;
                }
            }
            _ => {}
        }

        match (&preferences.author, &product.author) {
            (Some(a), Some(b)) => {
                if a.iter().find(|c| **c == b) {
                    prob *= 1.1;
                }
            }
            _ => {}
        }

        match (&preferences.size, &product.size) {
            (Some(a), Some(b)) => {
                if a.iter().find(|c| **c == b) {
                    prob *= 1.1;
                }
            }
            _ => {}
        }

        match (&preferences.color, &product.color) {
            (Some(a), Some(b)) => {
                if a.iter().find(|c| **c == b) {
                    prob *= 1.05;
                }
            }
            _ => {}
        }

        match &preferences.max_price {
            Some(p) => {
                if *p > price {
                    prob *= 1.1;
                }
            }
            _ => {}
        }

        match &preferences.min_price {
            Some(p) => {
                if *p < price {
                    prob *= 1.05;
                }
            }
            _ => {}
        }

        match &preferences.min_rating {
            Some(r) => {
                if *r < product.rating() {
                    prob *= 1.05;
                }
            }
            _ => {}
        }

        prob
    }*/
}
use crate::error::{Error, Result};
use crate::probability;
use common::DECIMAL_SCALE;
use futures::executor::block_on;
use metadata::dictionaries;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;

use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashSet;

use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct CSVProduct {
    pub name: String,
    pub category: String,
    pub subcategory: Option<String>,
    pub brand: Option<String>,
    pub price: Decimal,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Product {
    pub id: usize,
    pub name: u64,
    pub category: u64,
    pub subcategory: Option<u64>,
    pub brand: Option<u64>,
    pub price: Decimal,
    pub discount_price: Option<Decimal>,
    pub margin: f64,
    // affects rating
    pub satisfaction_ratio: f64,
    pub rating_count: usize,
    pub rating_sum: f64,
}

impl Product {
    pub fn final_price(&self) -> Decimal {
        self.discount_price.unwrap_or(self.price)
    }
}

pub struct ProductProvider {
    dicts: Arc<dictionaries::Provider>,
    org_id: u64,
    proj_id: u64,
    pub products: Vec<Product>,
    product_weight_idx: WeightedIndex<f64>,
    pub product_weights: Vec<f64>,
    pub promoted_products: Vec<usize>,
    promoted_product_weight_idx: WeightedIndex<f64>,
    pub deal_products: Vec<usize>,
    deal_product_weight_idx: WeightedIndex<f64>,
    pub categories: Vec<u64>,
    pub category_weight_idx: WeightedIndex<f64>,
    pub rating_weights: Vec<f64>,
}

impl ProductProvider {
    pub async fn try_new_from_csv<P: AsRef<Path>>(
        org_id: u64,
        proj_id: u64,
        rng: &mut ThreadRng,
        dicts: Arc<dictionaries::Provider>,
        path: P,
    ) -> Result<Self> {
        let mut rdr = csv::Reader::from_path(path)?;

        let mut products = Vec::with_capacity(1000);
        for (id, res) in rdr.deserialize().into_iter().enumerate() {
            let mut rec: CSVProduct = res?;
            rec.price.rescale(DECIMAL_SCALE as u32);
            let discount_price = if rng.gen::<f64>() < 0.3 {
                Some(rec.price * Decimal::new(9, 1))
            } else {
                None
            };

            let product = Product {
                id: id + 1,
                name: dicts
                    .get_key_or_create(org_id, proj_id, "event_product_name", rec.name.as_str())
                    .await?,
                category: dicts
                    .get_key_or_create(
                        org_id,
                        proj_id,
                        "event_product_category",
                        rec.category.as_str(),
                    )
                    .await?,
                subcategory: rec
                    .subcategory
                    .map(|v| {
                        block_on(dicts.get_key_or_create(
                            org_id,
                            proj_id,
                            "event_product_subcategory",
                            v.as_str(),
                        ))
                    })
                    .transpose()?,
                brand: rec
                    .brand
                    .map(|v| {
                        block_on(dicts.get_key_or_create(
                            org_id,
                            proj_id,
                            "event_product_brand",
                            v.as_str(),
                        ))
                    })
                    .transpose()?,
                price: rec.price,
                discount_price,
                margin: 0.,
                satisfaction_ratio: 0.0,
                rating_count: 0,
                rating_sum: 0.0,
            };
            products.push(product);
        }

        products.shuffle(rng);

        let product_weights =
            probability::calc_cubic_spline(products.len(), vec![1., 0.5, 0.3, 0.1])?;
        let product_weight_idx = WeightedIndex::new(&[1., 0.5, 0.3, 0.1]).unwrap();

        let promoted_products = products[0..5].iter().map(|p| p.id).collect();
        let promoted_product_weight_idx = WeightedIndex::new(&[1., 0.3, 0.2, 0.1, 0.1]).unwrap();

        let deal_products = products
            .iter()
            .filter(|p| p.discount_price.is_some())
            .map(|p| p.id)
            .collect();
        let deal_product_weight_idx = WeightedIndex::new(&[1., 0.3, 0.2, 0.1, 0.1]).unwrap();

        let mut categories = products
            .iter()
            .map(|p| p.category)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<u64>>();
        categories.shuffle(rng);

        let category_weight_idx = WeightedIndex::new(probability::calc_cubic_spline(
            categories.len(),
            vec![1., 0.5, 0.3, 0.1],
        )?)
        .map_err(|err| Error::External(err.to_string()))?;

        // make rating weights from 0 to 5 with 10 bins for each int value
        let rating_weights = probability::calc_cubic_spline(50, vec![0.01, 0.01, 0.1, 0.7, 1.])?;
        Ok(Self {
            dicts,
            org_id,
            proj_id,
            products,
            product_weights,
            product_weight_idx,
            promoted_products,
            promoted_product_weight_idx,
            deal_products,
            deal_product_weight_idx,
            categories,
            category_weight_idx,
            rating_weights,
        })
    }

    pub fn deal_product_sample(&self, rng: &mut ThreadRng) -> &Product {
        &self.products[self.deal_products[self.deal_product_weight_idx.sample(rng)]]
    }

    pub fn product_sample(&self, rng: &mut ThreadRng) -> &Product {
        &self.products[self.product_weight_idx.sample(rng)]
    }
    pub fn promoted_product_sample(&self, rng: &mut ThreadRng) -> &Product {
        &self.products[self.promoted_products[self.promoted_product_weight_idx.sample(rng)]]
    }

    pub fn get_product_by_id(&self, id: usize) -> &Product {
        &self.products[id]
    }
    pub fn rate_product(&mut self, id: usize, rating: f64) {
        let product = &mut self.products[id];
        product.rating_count += 1;
        product.rating_sum += rating;
    }

    pub async fn string_name(&self, key: u64) -> Result<String> {
        Ok(self
            .dicts
            .get_value(self.org_id, self.proj_id, "event_product_name", key)
            .await?)
    }
}

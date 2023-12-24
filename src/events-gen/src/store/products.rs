use std::collections::HashSet;
use std::io;
use std::sync::Arc;

use common::DECIMAL_SCALE;
use futures::executor::block_on;
use metadata::dictionaries;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::error::EventsGenError;
use crate::error::Result;
use crate::probability;

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
    pub name_str: String,
    pub category: u64,
    pub category_str: String,
    pub subcategory: Option<u64>,
    pub subcategory_str: Option<String>,
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
    pub fn path(&self) -> String {
        let name = self.name_str.replace(" ", "-").to_lowercase();
        name
    }
}

pub struct ProductProvider {
    dicts: Arc<dyn dictionaries::Provider>,
    properties: Arc<dyn metadata::properties::Provider>,
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
    pub fn try_new_from_csv<R: io::Read>(
        org_id: u64,
        proj_id: u64,
        rng: &mut ThreadRng,
        dicts: Arc<dyn dictionaries::Provider>,
        properties: Arc<dyn metadata::properties::Provider>,
        rdr: R,
    ) -> Result<Self> {
        let mut rdr = csv::Reader::from_reader(rdr);
        let mut products = Vec::with_capacity(1000);
        for (id, res) in rdr.deserialize().enumerate() {
            let mut rec: CSVProduct = res?;
            rec.price.rescale(DECIMAL_SCALE as u32);
            let discount_price = if rng.gen::<f64>() < 0.3 {
                Some(rec.price * Decimal::new(9, 1))
            } else {
                None
            };

            let product = Product {
                id: id + 1,
                name: dicts.get_key_or_create(
                    org_id,
                    proj_id,
                    properties
                        .get_by_name(org_id, proj_id, "Product Name")
                        .unwrap()
                        .column_name()
                        .as_str(),
                    rec.name.as_str(),
                )?,
                name_str: rec.name,
                category: dicts.get_key_or_create(
                    org_id,
                    proj_id,
                    properties
                        .get_by_name(org_id, proj_id, "Product Category")
                        .unwrap()
                        .column_name()
                        .as_str(),
                    rec.category.as_str(),
                )?,
                category_str: rec.category,
                subcategory: rec
                    .subcategory
                    .clone()
                    .map(|v| {
                        dicts.get_key_or_create(
                            org_id,
                            proj_id,
                            properties
                                .get_by_name(org_id, proj_id, "Product Subcategory")
                                .unwrap()
                                .column_name()
                                .as_str(),
                            v.as_str(),
                        )
                    })
                    .transpose()?,
                subcategory_str: rec.subcategory,
                brand: rec
                    .brand
                    .map(|v| {
                        dicts.get_key_or_create(
                            org_id,
                            proj_id,
                            properties
                                .get_by_name(org_id, proj_id, "Product Brand")
                                .unwrap()
                                .column_name()
                                .as_str(),
                            v.as_str(),
                        )
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
        let product_weight_idx = WeightedIndex::new([1., 0.5, 0.3, 0.1]).unwrap();

        let promoted_products = products[0..5].iter().map(|p| p.id).collect();
        let promoted_product_weight_idx = WeightedIndex::new([1., 0.3, 0.2, 0.1, 0.1]).unwrap();

        let deal_products = products
            .iter()
            .filter(|p| p.discount_price.is_some())
            .map(|p| p.id)
            .collect();
        let deal_product_weight_idx = WeightedIndex::new([1., 0.3, 0.2, 0.1, 0.1]).unwrap();

        let mut categories = products
            .iter()
            .map(|p| p.category)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<u64>>();
        categories.shuffle(rng);

        let category_weight_idx =
            WeightedIndex::new(probability::calc_cubic_spline(categories.len(), vec![
                1., 0.5, 0.3, 0.1,
            ])?)
            .map_err(|err| EventsGenError::Internal(err.to_string()))?;

        // make rating weights from 0 to 5 with 10 bins for each int value
        let rating_weights = probability::calc_cubic_spline(50, vec![0.01, 0.01, 0.1, 0.7, 1.])?;
        Ok(Self {
            dicts,
            properties,
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

    #[allow(dead_code)]
    pub fn get_product_by_id(&self, id: usize) -> &Product {
        &self.products[id]
    }

    #[allow(dead_code)]
    pub fn rate_product(&mut self, id: usize, rating: f64) {
        let product = &mut self.products[id];
        product.rating_count += 1;
        product.rating_sum += rating;
    }

    pub fn string_name(&self, key: u64) -> Result<String> {
        Ok(self.dicts.get_value(
            self.org_id,
            self.proj_id,
            self.properties
                .get_by_name(self.org_id, self.proj_id, "Product Name")
                .unwrap()
                .column_name()
                .as_str(),
            key,
        )?)
    }
}

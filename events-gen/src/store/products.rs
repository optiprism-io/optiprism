use std::collections::{HashMap, HashSet};
use std::env;
use std::ops::Div;
use std::path::Path;
use rand::distributions::WeightedIndex;
use rand::rngs::ThreadRng;
use rand::prelude::*;
use serde::Deserialize;
use crate::error::{Result, Error};
use rand::seq::SliceRandom;
use crate::probability;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use common::DECIMAL_SCALE;

pub struct Preferences {
    pub categories: Option<Vec<String>>,
    pub subcategories: Option<Vec<String>>,
    pub brand: Option<Vec<String>>,
    pub author: Option<Vec<String>>,
    pub size: Option<Vec<String>>,
    pub color: Option<Vec<String>>,
    pub max_price: Option<Decimal>,
    pub min_price: Option<Decimal>,
    pub min_rating: Option<f64>,
    pub has_coupon: bool,
}

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

#[derive(Debug, Clone)]
pub struct ProductStr<'a> {
    pub id: usize,
    pub name: &'a str,
    pub category: &'a str,
    pub subcategory: Option<&'a str>,
    pub brand: Option<&'a str>,
    price: Decimal,
    pub discount_price: Option<Decimal>,
}

impl Product {
    pub fn final_price(&self, has_coupon: bool) -> Decimal {
        self.discount_price.unwrap_or(self.price)
    }
}

pub enum Dict {
    ProductName = 0,
    Category = 1,
    Subcategory = 2,
    Brand = 3,
}

#[derive(Debug, Clone)]
pub struct Dicts {
    key_values: Vec<HashMap<u64, String>>,
    value_keys: Vec<HashMap<String, u64>>,
}

impl Dicts {
    pub fn new() -> Self {
        Self {
            key_values: vec![HashMap::default(); 4],
            value_keys: vec![HashMap::default(); 4],
        }
    }

    pub fn get_key_or_create(&mut self, dict: Dict, value: String) -> u64 {
        let usize_dict = dict as usize;
        if let Some(key) = self.value_keys[usize_dict].get(&value) {
            return *key;
        }

        let key = self.value_keys[usize_dict].len() as u64 + 1;
        self.value_keys[usize_dict].insert(value.clone(), key);
        self.key_values[usize_dict].insert(key, value);

        key
    }

    pub fn get_value(&self, dict: Dict, key: u64) -> &String {
        self.key_values[dict as usize].get(&key).unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct Products {
    pub dicts: Dicts,
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

impl Products {
    pub fn try_new_from_csv<P: AsRef<Path>>(path: P, rng: &mut ThreadRng) -> Result<Self> {
        let mut rdr = csv::Reader::from_path(path)?;
        let mut dicts = Dicts::new();
        let products = rdr.deserialize().into_iter().enumerate().map(|(id, p)| {
            match p {
                Ok(v) => {
                    let mut rec: CSVProduct = v;
                    rec.price.rescale(DECIMAL_SCALE as u32);
                    let discount_price = if rng.gen::<f64>() < 0.3 {
                        Some(rec.price * Decimal::new(9, 1))
                    } else {
                        None
                    };

                    Ok(Product {
                        id: id + 1,
                        name: dicts.get_key_or_create(Dict::ProductName, rec.name),
                        category: dicts.get_key_or_create(Dict::Category, rec.category),
                        subcategory: rec.subcategory.and_then(|v| Some(dicts.get_key_or_create(Dict::Subcategory, v))),
                        brand: rec.brand.and_then(|v| Some(dicts.get_key_or_create(Dict::Brand, v))),
                        price: rec.price,
                        discount_price,
                        margin: 0.,
                        satisfaction_ratio: 0.0,
                        rating_count: 0,
                        rating_sum: 0.0,
                    })
                }
                Err(e) => Err(e)
            }
        }).collect::<csv::Result<Vec<Product>>>()?;

        let product_weights = probability::calc_cubic_spline(products.len(), vec![1., 0.5, 0.3, 0.1])?;
        let product_weight_idx = WeightedIndex::new(&[1., 0.5, 0.3, 0.1]).unwrap();

        let promoted_products = products[0..5].iter().map(|p| p.id).collect();
        let promoted_product_weight_idx = WeightedIndex::new(&[1., 0.3, 0.2, 0.1, 0.1]).unwrap();

        let deal_products = products.iter().filter(|p| p.discount_price.is_some()).map(|p| p.id).collect();
        let deal_product_weight_idx = WeightedIndex::new(&[1., 0.3, 0.2, 0.1, 0.1]).unwrap();

        let mut categories = products
            .iter()
            .map(|p| p.category.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<u64>>();
        categories.shuffle(rng);

        let category_weight_idx = WeightedIndex::new(
            probability::calc_cubic_spline(categories.len(), vec![1., 0.5, 0.3, 0.1])?)
            .map_err(|err| Error::External(err.to_string()))?;


        // make rating weights from 0 to 5 with 10 bins for each int value
        let rating_weights = probability::calc_cubic_spline(50, vec![0.01, 0.01, 0.1, 0.7, 1.])?;
        Ok(Self {
            dicts,
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

    pub fn with_dict_values(&self, product: &Product) -> ProductStr {
        ProductStr {
            id: product.id,
            name: self.dicts.get_value(Dict::ProductName, product.name).as_ref(),
            category: self.dicts.get_value(Dict::Category, product.category).as_ref(),
            subcategory: product.subcategory.and_then(|v| Some(self.dicts.get_value(Dict::Subcategory, v).as_ref())),
            brand: product.brand.and_then(|v| Some(self.dicts.get_value(Dict::Brand, v).as_ref())),
            price: product.price,
            discount_price: product.discount_price,
        }
    }

    pub fn str_name(&self, id: u64) -> &str {
        self.dicts.get_value(Dict::ProductName, id as u64).as_ref()
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
}
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::Decimal128Builder;
use arrow::array::Int64Builder;
use arrow::array::TimestampSecondBuilder;
use arrow::array::UInt16Builder;
use arrow::array::UInt64Builder;
use arrow::array::UInt8Builder;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use common::DECIMAL_PRECISION;
use common::DECIMAL_SCALE;
use rust_decimal::Decimal;

use crate::error::Result;
use crate::store::events::Event;
use crate::store::profiles::Profile;
use crate::store::scenario::State;

pub struct RecordBatchBuilder {
    user_id: Int64Builder,
    created_at: TimestampSecondBuilder,
    event: UInt64Builder,
    product_name: UInt16Builder,
    product_category: UInt16Builder,
    product_subcategory: UInt16Builder,
    product_brand: UInt16Builder,
    product_price: Decimal128Builder,
    product_discount_price: Decimal128Builder,
    // search_query: UInt16Builder,
    spent_total: Decimal128Builder,
    products_bought: UInt8Builder,
    cart_items_number: UInt8Builder,
    cart_amount: Decimal128Builder,
    revenue: Decimal128Builder,
    country: UInt16Builder,
    city: UInt16Builder,
    device: UInt16Builder,
    device_category: UInt16Builder,
    os: UInt16Builder,
    os_version: UInt16Builder,
    schema: SchemaRef,
    len: usize,
}

impl RecordBatchBuilder {
    pub fn new(cap: usize, schema: SchemaRef) -> Self {
        Self {
            user_id: Int64Builder::with_capacity(cap),
            created_at: TimestampSecondBuilder::with_capacity(cap),
            event: UInt64Builder::with_capacity(cap),
            product_name: UInt16Builder::with_capacity(cap),
            product_category: UInt16Builder::with_capacity(cap),
            product_subcategory: UInt16Builder::with_capacity(cap),
            product_brand: UInt16Builder::with_capacity(cap),
            product_price: Decimal128Builder::with_capacity(cap),
            product_discount_price: Decimal128Builder::with_capacity(cap),
            // search_query: UInt16Builder::new(cap),
            spent_total: Decimal128Builder::with_capacity(cap),
            products_bought: UInt8Builder::with_capacity(cap),
            cart_items_number: UInt8Builder::with_capacity(cap),
            cart_amount: Decimal128Builder::with_capacity(cap),
            revenue: Decimal128Builder::with_capacity(cap),
            country: UInt16Builder::with_capacity(cap),
            city: UInt16Builder::with_capacity(cap),
            device: UInt16Builder::with_capacity(cap),
            device_category: UInt16Builder::with_capacity(cap),
            os: UInt16Builder::with_capacity(cap),
            os_version: UInt16Builder::with_capacity(cap),
            schema,
            len: 0,
        }
    }

    pub fn build_record_batch(&mut self) -> Result<RecordBatch> {
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.user_id.finish()),
            Arc::new(self.created_at.finish()),
            Arc::new(self.event.finish()),
            Arc::new(self.product_name.finish()),
            Arc::new(self.product_category.finish()),
            Arc::new(self.product_subcategory.finish()),
            Arc::new(self.product_brand.finish()),
            Arc::new(
                self.product_price
                    .finish()
                    .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?,
            ),
            Arc::new(
                self.product_discount_price
                    .finish()
                    .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?,
            ),
            Arc::new(
                self.spent_total
                    .finish()
                    .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?,
            ),
            Arc::new(self.products_bought.finish()),
            Arc::new(self.cart_items_number.finish()),
            Arc::new(
                self.cart_amount
                    .finish()
                    .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?,
            ),
            Arc::new(
                self.revenue
                    .finish()
                    .with_precision_and_scale(DECIMAL_PRECISION, DECIMAL_SCALE)?,
            ),
            Arc::new(self.country.finish()),
            Arc::new(self.city.finish()),
            Arc::new(self.device.finish()),
            Arc::new(self.device_category.finish()),
            Arc::new(self.os.finish()),
            Arc::new(self.os_version.finish()),
        ];

        let batch = RecordBatch::try_new(self.schema.clone(), cols)?;

        self.len = 0;
        Ok(batch)
    }

    pub fn write_event(
        &mut self,
        event: Event,
        event_id: u64,
        state: &State,
        profile: &Profile,
    ) -> Result<()> {
        // println!("event: {event}, time: {}", NaiveDateTime::from_timestamp(state.cur_timestamp, 0));
        self.user_id.append_value(state.user_id);
        self.created_at.append_value(state.cur_timestamp);
        self.event.append_value(event_id);

        match state.selected_product {
            None => {
                self.product_name.append_null();
                self.product_category.append_null();
                self.product_subcategory.append_null();
                self.product_brand.append_null();
                self.product_price.append_null();
                self.product_discount_price.append_null();
            }
            Some(product) => {
                self.product_name.append_value(product.name as u16);
                self.product_category.append_value(product.category as u16);
                self.product_subcategory
                    .append_option(product.subcategory.map(|v| v as u16));
                self.product_brand
                    .append_option(product.brand.map(|v| v as u16));
                self.product_price.append_value(product.price.mantissa());

                match product.discount_price {
                    None => self.product_discount_price.append_null(),
                    Some(price) => {
                        self.product_discount_price.append_value(price.mantissa());
                    }
                }
            }
        }

        if !state.spent_total.is_zero() {
            self.spent_total.append_value(state.spent_total.mantissa());
        } else {
            self.spent_total.append_null();
        }

        if !state.products_bought.is_empty() {
            self.products_bought
                .append_value(state.products_bought.len() as u8);
        } else {
            self.products_bought.append_null();
        }

        let mut cart_amount: Option<Decimal> = None;
        if !state.cart.is_empty() {
            self.cart_items_number.append_value(state.cart.len() as u8);
            let mut _cart_amount: Decimal = state
                .cart
                .iter()
                .map(|p| p.discount_price.unwrap_or(p.price))
                .sum();

            self.cart_amount.append_value(_cart_amount.mantissa());
            cart_amount = Some(_cart_amount);
        } else {
            self.cart_items_number.append_null();
            self.cart_amount.append_null();
        }

        match event {
            Event::OrderCompleted => {
                self.revenue.append_value(cart_amount.unwrap().mantissa());
            }
            _ => {
                self.revenue.append_null();
            }
        }

        self.country
            .append_option(profile.geo.country.map(|v| v as u16));
        self.city.append_option(profile.geo.city.map(|v| v as u16));
        self.device
            .append_option(profile.device.device.map(|v| v as u16));
        self.device_category
            .append_option(profile.device.device_category.map(|v| v as u16));
        self.os.append_option(profile.device.os.map(|v| v as u16));
        self.os_version
            .append_option(profile.device.os_version.map(|v| v as u16));

        self.len += 1;

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

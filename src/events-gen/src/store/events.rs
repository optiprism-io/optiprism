use enum_iterator::Sequence;
use strum_macros::Display;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, Display, Sequence)]
pub enum Event {
    #[strum(serialize = "User Registered")]
    UserRegistered,
    #[strum(serialize = "User Logged In")]
    UserLoggedIn,
    #[strum(serialize = "Subscribed For Newsletter")]
    SubscribedForNewsletter,
    #[strum(serialize = "Index Page Viewed")]
    IndexPageViewed,
    #[strum(serialize = "Deals Viewed")]
    DealsViewed,
    #[strum(serialize = "Product Searched")]
    ProductSearched,
    #[strum(serialize = "Not Found")]
    NotFound,
    #[strum(serialize = "Product Viewed")]
    ProductViewed,
    #[strum(serialize = "Product Added To Cart")]
    ProductAddedToCart,
    #[strum(serialize = "Buy Now Product")]
    BuyNowProduct,
    #[strum(serialize = "Product Rated")]
    ProductRated,
    #[strum(serialize = "Cart Viewed")]
    CartViewed,
    #[strum(serialize = "Coupon Applied")]
    CouponApplied,
    #[strum(serialize = "Customer Information Entered")]
    CustomerInformationEntered,
    #[strum(serialize = "Shipping Method Entered")]
    ShippingMethodEntered,
    #[strum(serialize = "Payment Method Entered")]
    PaymentMethodEntered,
    #[strum(serialize = "Order Verified")]
    OrderVerified,
    #[strum(serialize = "Order Completed")]
    OrderCompleted,
    #[strum(serialize = "Product Refunded")]
    ProductRefunded,
    #[strum(serialize = "Orders Viewed")]
    OrdersViewed,
    #[strum(serialize = "Session End")]
    SessionEnd,
}

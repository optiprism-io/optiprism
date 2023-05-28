mod funnel;
mod aggregated;

#[cfg(test)]
mod tests {
    type LargeUTF8Array = arrow2::array::Utf8Array<i64>;
    #[test]
    fn test () {
        let a = arrow2::array::Utf8Array::<i64>::from(vec![Some("a"), None]).boxed();

        let a = a.as_any().downcast_ref::<LargeUTF8Array>().unwrap();
        println!("{}",a.value(0));

    }
}
use std::sync::Arc;

use chrono::Duration;
use chrono::DurationRound;
use chrono::NaiveDateTime;

fn main() {
    let a1 = Arc::new(Int32Array::from_iter_values([-1, -1, 0, 3, 3])) as ArrayRef;
    let a2 = Arc::new(StringArray::from_iter_values(["a", "b", "c", "d", "d"])) as ArrayRef;
    let arrays = vec![a1, a2];

    // Convert arrays to rows
    let mut converter = RowConverter::new(vec![
        SortField::new(DataType::Int32),
        SortField::new(DataType::Utf8),
    ])
    .unwrap();
    let rows = converter.convert_columns(&arrays).unwrap();

    // Compare rows
    for i in 0..4 {
        assert!(rows.row(i) <= rows.row(i + 1));
    }
    assert_eq!(rows.row(3), rows.row(4));

    // Convert rows back to arrays
    let converted = converter.convert_rows(&rows).unwrap();
    assert_eq!(arrays, converted);

    // Compare rows from different arrays
    let a1 = Arc::new(Int32Array::from_iter_values([3, 4])) as ArrayRef;
    let a2 = Arc::new(StringArray::from_iter_values(["e", "f"])) as ArrayRef;
    let arrays = vec![a1, a2];
    let rows2 = converter.convert_columns(&arrays).unwrap();

    assert!(rows.row(4) < rows2.row(0));
    assert!(rows.row(4) < rows2.row(1));

    // Convert selection of rows back to arrays
    let selection = [rows.row(0), rows2.row(1), rows.row(2), rows2.row(0)];
    let converted = converter.convert_rows(selection).unwrap();
    let c1 = converted[0].as_primitive::<Int32Type>();
    assert_eq!(c1.values(), &[-1, 4, 0, 3]);

    let c2 = converted[1].as_string::<i32>();
    let c2_values: Vec<_> = c2.iter().flatten().collect();
    assert_eq!(&c2_values, &["a", "f", "c", "e"]);
}

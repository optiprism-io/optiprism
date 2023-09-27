pub struct Decimal {
    // Bits 0-15: unused
    // Bits 16-23: Contains "e", a value between 0-28 that indicates the scale
    // Bits 24-30: unused
    // Bit 31: the sign of the Decimal value, 0 meaning positive and 1 meaning negative.
    flags: u32,
    // The lo, mid, hi, and flags fields contain the representation of the
    // Decimal value as a 96-bit integer.
    hi: u32,
    lo: u32,
    mid: u32,
}

fn main() {
    let v: Vec<i128> = vec![
        0,
        10000000000000100,
        20000000000000200,
        30000000000000300,
        40000000000000400,
        50000000000000500,
        60000000000000600,
        70000000000000700,
        80000000000000800,
        90000000000000900,
        10000000000000100,
        20000000000000200,
        30000000000000300,
        40000000000000400,
        50000000000000500,
        60000000000000600,
        70000000000000700,
        80000000000000800,
        90000000000000900,
    ];

    let scale = 10_i128.pow(16);
    let b: i128 = v.iter().sum();
    println!("{}", b);
    println!("{}", b / 20);
    println!("{}", b / 20 / scale);
}

use chrono::Duration;
use chrono::DurationRound;
use chrono::NaiveDateTime;

fn main() {
    let dt = "2020-04-12 22:10:57".parse::<NaiveDateTime>().unwrap();
    let ts = dt.duration_trunc(Duration::days(1)).unwrap();
    println!("{ts}");
}

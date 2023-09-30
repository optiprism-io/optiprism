use chrono::Duration;
use chrono::DurationRound;
use chrono::Utc;

fn main() {
    let t = Utc::now();
    println!("{}", t.duration_trunc(Duration::days(4)).unwrap());
}

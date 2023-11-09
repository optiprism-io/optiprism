
running 1 test
test db::tests::it_works ... FAILED

successes:

successes:

failures:

---- db::tests::it_works stdout ----
thread 'db::tests::it_works' panicked at 'called `Result::unwrap()` on an `Err` value: "bad randomness source"', src/store/src/db/mod.rs:1063:43
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace


failures:
    db::tests::it_works

test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 6 filtered out; finished in 16.96s


fn main() {
    prost_build::compile_protos(
        &[
            "src/pb/account.proto",
            "src/pb/bookmark.proto",
            "src/pb/custom_event.proto",
            "src/pb/dashboard.proto"],
        &["src"],
    ).unwrap();
}
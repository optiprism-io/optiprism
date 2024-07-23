fn main () {
    prost_build::compile_protos(&["src/pb/account.proto"], &["src"]).unwrap();
}
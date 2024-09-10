fn main() {
    prost_build::compile_protos(&["src/pb/metadata.proto"], &["src"]).unwrap();
}

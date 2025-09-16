cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all --features run-docker-tests
cargo fmt

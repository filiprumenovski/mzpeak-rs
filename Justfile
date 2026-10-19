default:
    @just --list

build:
    cargo build --locked --release

test:
    cargo test --locked
    cargo test --locked --features mzml-parallel,tdf
    cargo test --locked --no-default-features

check:
    cargo fmt --package mzpeak -- --check
    cargo clippy --locked --all-targets --features mzml-parallel,tdf
    just test
    cargo doc --locked --no-deps --features mzml-parallel,tdf

bench:
    cargo run --locked --release --example io_hotpath

# Bundled rawr executable

`bin/aarch64-apple-darwin/rawr` is the release build of `rawr-ms-cli` for macOS
Apple Silicon, compiled from rawr-ms revision
`4c353b70f82b4edb01abbe1a433762541c2676ef` with Rust 1.98.1.
The SHA-256 and build target are recorded in `manifest.json`.

Source: https://github.com/filiprumenovski/rawr-ms (separate private repository).
No source from that repository is copied into mzpeak-rs.

To rebuild from a checkout of that exact revision:

```sh
cargo build --manifest-path /path/to/rawr-ms/Cargo.toml --locked --release -p rawr-ms-cli
cp /path/to/rawr-ms/target/release/rawr vendor/rawr-ms/bin/aarch64-apple-darwin/rawr
shasum -a 256 vendor/rawr-ms/bin/aarch64-apple-darwin/rawr
```

Update the manifest hash and `RAWR_MS_REVISION` in the Thermo conversion module
when changing the bundled revision. Re-run the process contract tests and an
actual RAW roundtrip. `MZPEAK_RAWR` and the Rust config's `rawr_binary` override
the bundle, allowing a separately built executable on Linux or Windows.
An override's revision is caller supplied and is not verified against the
bundled revision.

mzpeak invokes rawr with an argument array, never through a shell. It consumes
plain, uncompressed mzML from stdout, captures stderr separately, and checks
the final exit status. This avoids an mzML intermediate file while preserving
the exported f64 m/z values; rawr's community mzParquet export narrows m/z to
f32 and omits empty scans, so it is unsuitable as a lossless ingestion bridge.

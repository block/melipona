# Contributing

Open an issue for bugs or proposed changes, and submit pull requests against
`main`. Keep the core provider-neutral and put optional test workflows in examples.

Use current stable Rust. From a checkout, run:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo test --locked --all-targets --features serde_json/preserve_order,rustls/custom-provider
cargo test --locked --doc
```

Tests use local synthetic peers and require no API credentials or model weights.
The additional feature combination checks embedding in applications with different
JSON ordering and TLS defaults. Live endpoint testing is opt-in; follow the
[scenario guide](scenarios/README.md) and report provider limitations accurately.

Include a focused regression test for changes to protocol behavior. Do not commit
credentials, private endpoints, recordings, model artifacts, or session output.
Please remove sensitive content from bug reports and logs before sharing them.

Contributions are made under the repository's [Apache 2.0 license](LICENSE).

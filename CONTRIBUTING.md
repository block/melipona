# Contributing

Open an issue for bugs or proposed changes, and submit pull requests against
`main`. Keep the core provider-neutral and put optional test workflows in examples.
A provider extension belongs in the core only if it is off by default, enabled by
explicit configuration, used only after the server advertises it, and limited to
an outbound allowlist, as the Frankie playback extension is.

Use current stable Rust. From a checkout, run:

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked --all-targets
cargo test --locked --all-targets --features serde_json/preserve_order,rustls/custom-provider
cargo test --locked --doc
cargo clippy --locked --all-targets --features mcp -- -D warnings
cargo test --locked --all-targets --features mcp
cargo test --locked --all-targets --features mcp,serde_json/preserve_order,rustls/custom-provider
cargo test --locked --doc --features mcp
```

Tests use local synthetic peers and require no API credentials or model weights.
The additional feature combination checks embedding in applications with different
JSON ordering and TLS defaults. Live endpoint testing is opt-in; follow the
[scenario guide](scenarios/README.md) and report provider limitations accurately.

Include a focused regression test for changes to protocol behavior. Do not commit
credentials, private endpoints, recordings, model artifacts, or session output.
Please remove sensitive content from bug reports and logs before sharing them.

Contributions are made under the repository's [Apache 2.0 license](LICENSE).

MCP subprocess fixtures require `python3` on Unix. The real server qualification
is opt-in and writes receipts outside this repository:

```sh
MELIPONA_TEST_MCP_BINARY=/path/to/buzz-dev-mcp \
MELIPONA_TEST_ARTIFACT_DIR=/path/to/new/qualification-directory \
cargo test --locked --all-targets --features mcp -- --include-ignored
```

Build that server from the intended Buzz revision and record its commit alongside
the Melipona commit. This exercises local tools and cleanup, not live voice/model
qualification. Never include private server configuration or test receipts in a PR.

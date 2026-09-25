# Testing and compatibility

```sh
cargo test --locked --all-targets
cargo test --locked --all-targets --features mcp
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --all -- --check
```

The optional [soak runner](../scenarios/README.md) tests persistent sessions with
text, images, paced PCM audio, actual tools, interruptions, and growing context:

```sh
cargo run --locked --release --example soak -- scenarios/text-context.json /tmp/melipona-run-1
```

Use a new output directory for each run. Results can contain prompts, transcripts,
and tool data; keep them outside the repository. The runner reports missing
measurements explicitly and measures virtual playback, not physical audio devices.

The optional conformance probe checks an endpoint against the GA reference. Every
server event it sees is checked for the reference's required fields, and each check
drives one documented flow on a fresh session: lifecycle order, request metadata,
cancellation, out-of-band responses, error correlation, item create/retrieve/delete,
tool calls, manual audio commit/clear, and truncation of heard audio:

```sh
cargo run --locked --example conformance            # or name individual checks
```

It prints one JSON line per check (`pass`, `fail`, or `skip` with a reason), then a
summary with counts. It exits 0 only if every selected check passed, and reports
`conformant` only when all checks ran and passed. That covers these flows, not the
whole reference.

## Compatibility and scope

Melipona targets the GA OpenAI Realtime WebSocket event format. Provider features
and turn policies vary; use the conformance probe and scenarios to qualify an endpoint.
A compatible text-completions API alone does not establish Realtime compatibility.

Automated tests use local WebSocket/TLS peers and synthetic fixtures. They cover
streaming media, tools, cancellation, backpressure, timeouts, and protocol errors.
Live development tests exercised an MTPLX/Frankie endpoint, but do not establish
universal provider conformance, full-context reliability, or speech quality.
Frankie playback extensions are optional and require an advertised capability.

Protocol references: [Realtime conversations](https://developers.openai.com/api/docs/guides/realtime-conversations)
and [WebSockets](https://developers.openai.com/api/docs/guides/voice-websockets).


---

[Overview](../README.md) · [Contributing](../CONTRIBUTING.md)

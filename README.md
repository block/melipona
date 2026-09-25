# Melipona

**A minimal realtime voice agent harness.**

Written in Rust. Full-duplex audio, text, images, and tool calling over a persistent
OpenAI Realtime-compatible WebSocket connection.

Melipona connects your application's tools and audio player to a realtime model.
It keeps audio flowing while tools run, tracks completed calls to avoid duplicate
execution, and handles interruption without confusing stopped speech with
cancelled work. Use it as a library or a small JSONL command-line host.

The inference endpoint supplies the model, transcription, speech synthesis, and
turn detection. Your application supplies audio capture/playback and any tool
permissions. Melipona includes no model weights, inference server, or audio UI.

## Quick start

Install current stable Rust, then build from this repository:

```sh
git clone https://github.com/block/melipona.git
cd melipona
cargo build --locked
export REALTIME_URL=ws://127.0.0.1:8080/v1/realtime
export REALTIME_SESSION='{"type":"realtime","output_modalities":["text"]}'
cargo run --locked -- --echo-tool
```

Start a compatible endpoint separately. For a remote endpoint, use `wss://` with
verified certificates. Plain `ws://` is accepted only for loopback hosts.

Enter these commands on separate lines, keeping stdin open for the response:

```json
{"command":"text","text":"Call echo with text hello, then summarize its result."}
{"command":"respond"}
```

The CLI writes JSONL events to stdout. `--echo-tool` registers one harmless tool;
the default registry is empty. Send `{"command":"close"}`, press Ctrl+C, or close
stdin to finish. The CLI does not capture microphone input or play audio.

| Environment variable | Purpose |
| --- | --- |
| `REALTIME_URL` | Required WebSocket endpoint. |
| `REALTIME_MODEL` | Optional model query parameter. |
| `REALTIME_TOKEN` | Optional bearer token, sent in the Authorization header. |
| `REALTIME_SESSION` | Session configuration JSON; tools come from the registry. |
| `REALTIME_MCP` | Optional local `mcpServers` JSON configuration; requires the `mcp` build feature. |
| `REALTIME_TOOL_CONTINUATION` | `client` (default) or `server`: who requests a response after tool results. |
| `REALTIME_ECHO_DELAY_MS` | Optional echo-tool delay, from 0 to 10,000 ms. |
| `REALTIME_FRANKIE_EXTENSIONS` | Set to `1` to opt into advertised Frankie playback extensions. |

## Embed in an application

This text-only example opens one session and waits for its response. Applications
using audio must also implement the [playback contract](docs/protocol.md).

```rust,no_run
use melipona::{Command, Config, Event, Session, ToolRegistry};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = Config::new("ws://127.0.0.1:8080/v1/realtime");
    config.session = json!({"type":"realtime", "output_modalities":["text"]});
    let mut session = Session::connect(config, ToolRegistry::empty()).await?;
    session.handle.send(Command::Text { text: "Hello".into() })?;
    session.handle.send(Command::Respond { response: None })?;

    while let Some(event) = session.events.recv().await {
        if let Event::Server { event } = event {
            if event["type"] == "response.output_text.delta" {
                print!("{}", event["delta"].as_str().unwrap_or(""));
            }
            if event["type"] == "response.done" {
                break;
            }
        }
    }
    session.finish().await?;
    Ok(())
}
```

Register function definitions and a trusted asynchronous `ToolExecutor` with
`ToolRegistry::new`. Melipona validates arguments against your JSON Schemas,
executes completed calls, returns results, and coordinates continuation. The
executor owns authorization and side effects. Blocking work belongs in
`spawn_blocking`, not on a Tokio worker. See the [echo executor](src/main.rs).

Commands also support base64 audio chunks, manual audio commit/clear, images,
response interruption with heard-audio positions, and individual tool cancellation.
`respond` accepts any `response.create` parameters, including parallel out-of-band
responses (requests are acknowledged one at a time), and `event` forwards any other Realtime client event, so every GA client
event is reachable. Text and image insertion do not implicitly request a response. With server VAD,
the provider normally commits audio and responds; with turn detection disabled,
send `commit_audio` followed by `respond`.

## Local MCP tools (optional)

Build with `cargo build --locked --features mcp`. Supply one or more **local stdio**
servers in `REALTIME_MCP`; no UI or separate agent is required:

```sh
export REALTIME_MCP='{"mcpServers":{"dev":{"command":"buzz-dev-mcp","args":[],"tools":["shell","read_file"]}}}'
cargo run --locked --features mcp
```

Install the configured server separately. This example uses
[buzz-dev-mcp](https://github.com/block/buzz/tree/main/crates/buzz-dev-mcp),
not a linked Buzz dependency. `command` is an executable, not a shell expression;
`args`, `env`, `cwd`, and `type:"stdio"` are optional. Other transports are rejected.
Unrelated top-level configuration keys are ignored; unknown server fields are
rejected. Server keys use `[a-z0-9-]{1,16}`. The optional
`tools` list uses **original MCP names**; omit it to expose every listed tool, or
use `[]` to expose none. Unknown selected tools and unsupported selected schemas
fail startup, rather than silently losing validation. `--echo-tool` cannot be
combined with `REALTIME_MCP`. A build without the feature rejects this variable.

**Choosing a server authorizes the model to call its exposed tools without asking.**
Only use trusted launch configuration and executables. MCP is neither a sandbox
nor protection against prompt injection: tools run with the host user's privileges,
and `cwd` does not restrict filesystem access. The child inherits the normal host
environment except `REALTIME_*`; its explicit `env` entries are then applied.
Other inherited credentials are **not** isolated. Tool descriptions and results
are server-supplied data, not trusted instructions.

The catalog is fixed at connection time. Names are advertised as `server__tool`
where possible, otherwise with a stable sanitized/hash alias; calls route by an
explicit map to the original name. Schemas remain unchanged. Text,
`structuredContent`, and `isError` survive result mapping; binary blocks are
represented by omission descriptors, not delivered to the model as images/audio.
Oversized results become explicitly truncated head/tail previews with error status
retained: readable text when available, otherwise labelled JSON. Structured data
is not preserved as structured data after truncation. There is no transparent
replay or restart after an uncertain failure.

The adapter uses rmcp for negotiation, framing, request IDs and cancellation.
It supports ordinary local stdio tool calls, not remote MCP, dynamic catalog
updates, sampling, roots, elicitation or task-required tools. A standards-compliant
server requiring one of those capabilities is outside this version's scope.
Transport frames, discovery pages/count/bytes and startup are bounded; the
existing session bounds concurrency, arguments, output and tool deadlines. Servers
start sequentially; the default startup limit is 15 seconds per server, up to
16 servers (240 seconds total).

Library hosts use `mcp::Mcp::connect`, `registry()` (or `tools()` plus a wrapped
`executor()`), and `catalog()` for original metadata including annotations.
Annotations are hints, not authorization. Applications can wrap the executor to
implement their own approval policy; Melipona contains no approval UI.
Prefer finishing sessions before awaiting `mcp.shutdown()`; shutting down the
adapter first also cancels still-live calls safely:

```rust,no_run
# #[cfg(feature = "mcp")]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use melipona::{Config, Session, mcp::{Mcp, McpConfig, McpLimits}};
let config = Config::new("ws://127.0.0.1:8080/v1/realtime");
let servers: McpConfig = serde_json::from_str(&std::env::var("REALTIME_MCP")?)?;
let mcp = Mcp::connect(servers, McpLimits::default(), config.limits.tool_result_bytes).await?;
let session = Session::connect(config, mcp.registry()?).await?;
// Drive the session and drain its events in your application.
let finished = session.finish().await;
mcp.shutdown().await?;
finished?;
# Ok(())
# }
```

Speech interruption leaves tools running. Explicit cancellation or dropping a
pending executor future requests MCP cancellation; this is advisory, not rollback.
Normal shutdown cancels live adapter calls, gives cancellation notifications and cooperative process exit a
bounded grace period, then terminates the owned process group on Unix and reaps
its leader. Descendants that create new process groups or sessions are the
server's responsibility. Dropping the owner without shutdown uses immediate
termination; a forced host kill cannot run cleanup. Windows uses a Job Object,
but Windows process cleanup has not been live-qualified by the Unix tests.
Do not equate an uncertain transport failure with a tool that never ran.

**Known provider limitation:** live Frankie tests rejected tool results inserted
while another response was active, both with MCP and with the built-in echo tool.
The tested background-task option did not resolve this. Ordinary tool-result
acknowledgement followed by spoken continuation passed; overlapping result
insertion remains unqualified ([tracked separately](https://github.com/block/melipona/issues/3)).
No retry or coordinator workaround is added here.

## Session behavior

- Audio and tool execution proceed independently. Interrupting speech leaves
  committed tools running unless the application explicitly cancels them.
- The endpoint owns turn detection. Melipona does not cancel merely because
  speech started, so a compatible endpoint can preserve human backchannels.
- Playback uses actual rendered-sample positions. A separate cancellation watch
  invalidates queued audio even when the event consumer is behind.
- Queues, tool concurrency, message sizes, and session ledgers are bounded.
  Connections, writes, tools, and acknowledgements have deadlines. A malformed,
  oversized, or excess tool call gets an error result; the conversation continues.
- There is no automatic reconnect, write replay, or tool retry. Call deduplication
  lasts for one bounded session; it is not durable exactly-once execution.

See [the protocol guide](docs/protocol.md) for ownership, limits, shutdown,
interruption, and optional provider extensions.

## Repeatable testing

```sh
cargo test --locked --all-targets
cargo test --locked --all-targets --features mcp
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --all -- --check
```

The optional [soak runner](scenarios/README.md) tests persistent sessions with
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

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md) and [GOVERNANCE.md](GOVERNANCE.md).
Melipona is licensed under [Apache 2.0](LICENSE). Dependencies retain their own
licenses and notices; binary distributors must include applicable third-party
notices, including ring and the webpki-roots certificate data.

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
    session.handle.send(Command::Respond)?;

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
Text and image insertion do not implicitly request a response. With server VAD,
the provider normally commits audio and responds; with turn detection disabled,
send `commit_audio` followed by `respond`.

## Session behavior

- Audio and tool execution proceed independently. Interrupting speech leaves
  committed tools running unless the application explicitly cancels them.
- The endpoint owns turn detection. Melipona does not cancel merely because
  speech started, so a compatible endpoint can preserve human backchannels.
- Playback uses actual rendered-sample positions. A separate cancellation watch
  invalidates queued audio even when the event consumer is behind.
- Queues, tool concurrency, message sizes, and session ledgers are bounded.
  Connections, writes, tools, and acknowledgements have deadlines.
- There is no automatic reconnect, write replay, or tool retry. Call deduplication
  lasts for one bounded session; it is not durable exactly-once execution.

See [the protocol guide](docs/protocol.md) for ownership, limits, shutdown,
interruption, and optional provider extensions.

## Repeatable testing

```sh
cargo test --locked --all-targets
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

## Compatibility and scope

Melipona targets the GA OpenAI Realtime WebSocket event format. Provider features
and turn policies vary; use the included scenarios to qualify an endpoint.
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

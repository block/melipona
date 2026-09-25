<p align="center">
  <img src="docs/assets/melipona.png" width="360" alt="Papercraft bee speaking and listening on a flower-shaped candlestick telephone">
</p>

<h1 align="center">Melipona</h1>

<p align="center">
  <strong>A minimal realtime voice agent harness.</strong><br>
  Written in Rust. Built for the open line.
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> ·
  <a href="#embed-in-an-application">Embed</a> ·
  <a href="#local-mcp-tools">Local MCP</a> ·
  <a href="docs/protocol.md">Protocol</a>
</p>

---

## Keep the conversation open

**Audio keeps flowing.** Full-duplex audio, text, images, and tool calls share a
persistent OpenAI Realtime-compatible WebSocket connection.

**Tools have their own lifecycle.** Completed calls are validated and deduplicated
within a bounded session. Stopping speech does not cancel committed work.

**Your application stays in control.** Bring your audio capture, playback, and tool
permissions. The endpoint supplies the model, transcription, speech synthesis,
and turn detection. Melipona includes no model weights, inference server, or audio UI.

```text
Your app
mic + player
     |
Melipona ----- Your tools
session       ToolExecutor / MCP
+ playback
     |
Realtime endpoint
model + voice
```

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

[All environment variables →](docs/usage.md)

---

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

## Local MCP tools

Use local tools without a second agent loop. The optional `mcp` feature connects
stdio servers to the same tool registry and executor used by the Rust API.

> [!WARNING]
> Choosing a server authorizes the model to call its exposed tools **without asking**.
> Tools run with your user privileges. MCP is not a sandbox, and a working directory
> does not restrict file access. Inherited credentials other than `REALTIME_*` are
> not isolated. Only use trusted configuration and executables.
> Tool descriptions and results are server-supplied data, not trusted instructions;
> MCP does not protect against prompt injection.

With a compatible endpoint configured and `buzz-dev-mcp` installed separately:

```sh
export REALTIME_MCP='{
  "mcpServers": {
    "dev": {
      "command": "buzz-dev-mcp",
      "tools": ["shell", "read_file"]
    }
  }
}'
cargo run --locked --features mcp
```

MCP is off by default. Do not combine `REALTIME_MCP` with `--echo-tool`.
The catalog is fixed when you connect; only local stdio transport is supported.
Cancellation requests are advisory, not rollback.

**Provider limits still apply.** The tested Frankie endpoint rejected tool results
arriving during another active response. Ordinary tool-result acknowledgement and
spoken continuation passed; overlapping insertion remains unqualified
([issue #3](https://github.com/block/melipona/issues/3)).

[Configuration, security, result mapping, and shutdown →](docs/mcp.md)

---

## A small surface. An explicit contract.

Melipona targets the GA OpenAI Realtime WebSocket event format, not ordinary
text-completions APIs. Provider features and turn policies vary. Qualify your
endpoint; synthetic tests and individual live passes are not universal conformance.
There is no automatic reconnect, write replay, or tool retry, and session
call deduplication is not durable exactly-once execution.

| Read next | What you will find |
| :--- | :--- |
| [Session & playback](docs/protocol.md) | Interruption, heard-audio positions, continuation, limits. |
| [CLI & session reference](docs/usage.md) | Environment variables and session behavior. |
| [Local MCP](docs/mcp.md) | Server configuration, permissions, result mapping, cleanup. |
| [Testing & compatibility](docs/testing.md) | Full suites, conformance checks, provider caveats. |
| [Scenario guide](scenarios/README.md) | Repeatable text, audio, image, and tool workflows. |
| [Contributing](CONTRIBUTING.md) | Development checks and contribution guidelines. |

## Contributing and license

See [CONTRIBUTING.md](CONTRIBUTING.md) and [GOVERNANCE.md](GOVERNANCE.md).
Melipona is licensed under [Apache 2.0](LICENSE). Dependencies retain their own
licenses and notices; binary distributors must include applicable third-party
notices, including ring and the webpki-roots certificate data.

<p align="center"><sub>One open line. Clear boundaries.</sub></p>

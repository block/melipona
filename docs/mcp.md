# Local MCP tools

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

---

[Overview](../README.md) · [Session contract](protocol.md)

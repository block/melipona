# CLI and session reference

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

See [the protocol guide](protocol.md) for ownership, limits, shutdown,
interruption, and optional provider extensions.


[Overview](../README.md) · [Testing and compatibility](testing.md)

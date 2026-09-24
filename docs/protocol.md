# Session and playback contract

- A completed `response.output_item.done` function item commits a tool.
  Fragments and `function_call_arguments.done` alone never dispatch it.
  Completed `response.done` output reconciles missing item events.
- Identical call IDs and canonical arguments execute once per session.
  Conflicting arguments/name/response owner fail the session. This is bounded
  session deduplication, not durable exactly-once execution.
- A model mistake fails only its own call. Invalid JSON, schema violations,
  unknown tools, arguments over the size limit and calls beyond the concurrency
  limit each receive an error result without executing; the session continues.
- Results are `function_call_output` items. Client continuation waits for the
  response to finish successfully and for all results to be accepted, then
  sends one `response.create`. `Continuation::Server` delegates this entirely
  to the endpoint. Rejections never trigger automatic retries.
- `speech_started` alone cancels nothing. The endpoint owns turn policy.
  Cancelled responses invalidate queued media; incomplete/failed responses keep
  valid partial audio but do not auto-continue tools. Speech cancellation leaves
  committed tools running.
- A playback host must watch `Session.playback` and check `allows(response_id)`
  immediately before rendering queued frames. Clear stopped responses; terminal
  state invalidates every queued frame independently of the event FIFO.
- Stop the player first. Send `interrupt` with final rendered-sample positions,
  or `playback_stopped` after server cancellation. The harness truncates each
  audio part at its heard position. Omitted parts are treated as unheard.
  Stopping never depends on truncation support: with
  `Capabilities.truncate = false` the harness still validates positions, cancels
  and invalidates playback, but the endpoint keeps the unheard text in context.
  Generated duration is only a validation bound. PCM/G711 bounds retain each
  part's accepted format; a format change within one part is rejected.
- If generation has finished while playback remains buffered, the host still
  needs an explicit interruption when its turn policy requires it. A generic
  client cannot infer whether a short utterance is a backchannel.

Controls have independent bounded admission and an urgent socket lane. They can
overtake queued media, but not a frame already being written. Audio/commit retain
ordinary FIFO order. Full command queues reject admission; full internal queues
terminate instead of silently dropping audio or accumulating stale playback.

Defaults: 64 ordinary messages per queue, 8 priority commands, 16 priority writes,
64 KiB per encoded audio chunk, 8 MiB per message, 16 concurrent tools, 64 KiB tool
arguments/results, 4,096 lifetime calls/audio parts and 8,192 responses. Only
the lifetime ledgers and host-side queues end a session when exhausted. Large
images can still occupy the message-size bound per slot; tune `Limits` for your
host and use small paced audio frames. Lifetime ledgers retain IDs until the
session ends at their cap.

Connection, initialization, writes, result acknowledgements and tool work have
deadlines. A ping watchdog detects silent half-open connections. Cancellation,
timeout and shutdown cannot undo external side effects. A sent result may have
been accepted despite a lost acknowledgement. There is no reconnect, write
replay or tool retry. A new connection is a new session; the host must reconcile
uncertain external actions before retrying anything.

## Optional Frankie playback extensions

Default behavior uses standard Realtime events. Set `Config.frankie_extensions`
(CLI: `REALTIME_FRANKIE_EXTENSIONS=1`) to allow
`{"command":"frankie","event":{...}}` after the server advertises
`session.frankie.playback_feedback: true`. The outbound allowlist is:

- `input_audio_buffer.append` with aligned base64 `audio`/`playback` frames;
- `frankie.playback.position`/`frankie.playback.finished` with item/response IDs,
  actual `audio_end_ms` and the endpoint's `queued_samples`.

These are provider extensions. Incoming clear/pause/resume events remain raw
server events: the host owns player actions and maps a clear to
`playback_stopped`. Full Frankie extension parity is not claimed. Session options
are explicit; builds that reject results during active responses must enable
their background-result capability or reject that workflow.

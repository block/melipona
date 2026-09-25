# Reproducible persistent-session qualification

The optional Rust `soak` example drives the same `Session`, tool executor and
playback contract used by an application. It never opens another provider socket,
starts inference, changes a server, or runs shell commands chosen by the model.
The normal library and JSONL host acquire no new dependency or runtime feature.

```sh
export REALTIME_URL=ws://127.0.0.1:8080/v1/realtime
# Optional: REALTIME_MODEL, REALTIME_TOKEN, REALTIME_SESSION (JSON),
# REALTIME_TOOL_CONTINUATION=client|server.
cargo run --locked --release --example soak -- scenarios/text-context.json /tmp/soak-run-1
```

The output directory must be new. On Unix it is private (0700). Keep output outside
the source tree: evidence contains your prompts, transcripts, tool arguments and
provider events. Bearer credentials and endpoint URLs are not written; recognized
secret fields are redacted, but provider-specific events can contain private data.
No endpoint is contacted until you explicitly supply `REALTIME_URL`. Remote plain
WebSockets are rejected by the core; use verified WSS or a loopback SSH tunnel.

`REALTIME_SESSION` replaces the scenario's session JSON completely. This lets the
same scenario run against different engine builds without provider-name branches.
Record the model artifact/build/quantization, voice, MTP, cache/context capacity and
load externally alongside each run. The evidence records resolved session options,
model name, seed, fixture/binary checksums and crate version; it cannot infer server build
or GPU settings. Compare matched settings and repeat in separate directories.

## Scenarios

- `text-context.json`: starts with no filler, then appends 4,096 deterministic ASCII
  characters per turn on one persistent session, alternating a long answer and a
  real harmless tool call. It stops at 100,000 **reported** input tokens or an exact
  configured capacity-error code. Change the target to your advertised capacity
  minus output headroom. Data is varied rather than one repeated token. No token
  count is inferred from characters.
- `voice.json`: manual PCM commits, streamed spoken paragraphs, and tool results,
  repeated for 30 turns. Generate the question fixture below first.
- `overlap.json`: paced PCM acknowledgement versus a genuine correction during
  rendered output, repeated five times. It intentionally fails a server that cuts
  on every acknowledgement. The maximum-stop setting is a chosen acceptance gate,
  not a claimed performance result. The runner never cancels on `speech_started`.

All scenario fields reject unknown keys. `rounds` repeats the ordered `turns`
without clearing history. Optional `setup` inserts one initial user message exactly
once, before any response, for long-range answer-retention probes. The context
scenario recalls this early fact repeatedly without re-inserting it. Every turn accepts `text`, `pcm`, and/or a local PNG/JPEG
`image`; text accompanying an image is sent in the same user item. PCM is raw mono
signed little-endian 16-bit, 24 kHz, at most 120 seconds. Send recordings or synthetic
speech, not a WAV header. The runner does no resampling or transcription. A
`server_vad` audio turn lets the provider commit/respond; otherwise the runner
commits/responds after its last sample. Server-VAD scenarios must configure VAD.
Continuous silence/reference frames are sent only for server-VAD/overlap scenarios.
Manual input sends PCM only during the recorded utterance; manual text requests
never fill an audio buffer with silence while the model works.

For example, on macOS create newly synthesized public test speech:

```sh
mkdir -p scenarios/fixtures
say -r 175 -o /tmp/question.aiff 'What is two plus two? Say the answer in words.'
ffmpeg -i /tmp/question.aiff -ar 24000 -ac 1 -f s16le scenarios/fixtures/question.pcm
```

Create `acknowledgement.pcm` saying "Mm hmm" and `correction.pcm` saying
"Stop. What is three plus three? Say the answer in words." similarly. Trim leading
silence consistently and retain the same files across comparisons. Public
repositories should contain only fixtures you have rights to redistribute.
These examples do not download or publish recordings. Synthetic caller results
are not a substitute for testing diverse natural human voices.

An overlap begins after `after_played_ms` actual virtual rendered samples, not
generated duration. `interrupt: true` explicitly exercises host cancellation and
exact heard truncation; its default `false` tests the server's acoustic policy.
`expect_clear` is required. The fixture clock starts at the first fixture sample;
leading silence is included in stop latency, so record/trim it deliberately. PCM
continues through the stop and the provider can answer the correction as a fresh
turn. Setting `echo_delay_ms` to 5000 tests actual slow execution; audio continues
while the executor waits. `expect.tool_texts` requires the exact multiset of real
executions, catching omissions and repeated calls, including across later turns.

## Optional Frankie behavior

Set `frankie: true` in your scenario only when testing an endpoint that advertises
`session.frankie.playback_feedback: true`. Configure any required Frankie session
options explicitly via `REALTIME_SESSION`. The runner then sends aligned microphone
and rendered-reference frames plus `frankie.playback.position`/`finished` through
the existing extension allowlist. This is separate from standard Realtime.
Unsupported capability or PCM format is an explicit skip, never a silent fallback.
A server playback clear stops only its named response. A spoken correction waits
for the latest VAD capture to end and its fresh response to complete, even when
phrase pauses cause intermediate replies to be cancelled. Cancellation counts are
reported; cancellation alone does not finish a capture. Semantic checks exclude
cleared/cancelled drafts and, for genuine interruption, score only the latest
completed post-interruption reply. A reply cut short by its output-token budget retains its delivered text for diagnosis but still fails for incomplete status; it is not mislabeled zero-word collapse. A pause is reported as a
failed uninterrupted-backchannel qualification rather than hidden by a client
policy. Generation completion never means queued audio was already heard.

## Results and limits

`events.jsonl` streams timestamped provider/control/executor evidence;
`summary.json` includes per-turn outcomes and measurements. Failures return a
nonzero exit code. An exact configured capacity refusal after successful turns is
a successful capacity-boundary observation, not successful execution of that last
request. For endpoints with no error code, `context.capacity_messages` can name
an exact known context-error message; substring/heuristic matching is never used.
Refusal on the first turn fails. Finishing the configured turns without
verifying a requested token target fails. Providers that truncate history may
plateau below the target; inspect the per-turn reported input/cache counts rather
than calling that full-context success. Automatic retries/reconnects are absent.

Measurements include input-end to first public text, first received/rendered audio,
response length, rendered audio duration, playback gaps (including partial-frame missing samples), scheduling lateness,
overlap-start to playback stop, tool executions, and provider-reported token/cache
usage. Missing counters are `null` with explicit skip reasons. Text delivery rate
requires timed text deltas and reported text-token usage; it is **not GPU decode
speed**. Audio transcript timing never masquerades as brain token timing. The
latest raw usage object is retained for provider-specific analysis. Optional
`telemetry` maps a declared provider event's JSON pointers into reported counters:

```json
{"telemetry":{"event":"example.metrics","input_tokens":["/metrics/prompt_tokens"],"cached_input_tokens":"/metrics/cached_tokens","decode_tokens_per_second":"/metrics/decode_tok_s","peak_memory_bytes":"/metrics/peak_memory_bytes"}}
```

Multiple `input_tokens` pointers are summed only when they represent explicitly
known disjoint categories (for example cached plus newly evaluated prompt tokens).
Any missing category leaves the total unknown. The source mapping is recorded;
these are provider-reported counters, not independently measured GPU or process
memory. Standard `response.done.usage` remains supported without a mapping.
Last-response provider rates are distinct from the client's text-delivery rate;
client delivery rates are omitted for multi-response tool turns.

The virtual player uses a 32 ms sample clock and checks the core's cancellation
watch before every frame. Timings include client scheduling and network delay;
they are not physical speaker/microphone loopback measurements. Large tick
lateness invalidates a fine-grained latency comparison. Leading/trailing padded
PCM and onset conventions must match across runs. Input and output use one clock;
the player does not drain generated audio instantaneously.

Per-turn minimum-word and substring checks detect known short-reply/collapse
regressions. They are not general intelligence, prosody, speech intelligibility,
or semantic task-quality scores. Set `record_audio: true` to stream actual virtual playback to `turn-NNNN.pcm`
files for independent transcription/listening. Files contain mono PCM16 at 24 kHz,
begin at first playback, retain subsequent silence/gaps, and exclude cancelled
queued audio. Recording is opt-in and has a 1 GiB per-run disk cap; no whole-run
PCM buffer is retained. Convert with `ffmpeg -f s16le -ar 24000 -ac 1 -i
turn-0000.pcm turn-0000.wav`. The runner does not inspect process memory; collect
explicitly authorized provider telemetry externally.

Bounds: 4,096 total turns, 128 responses/tools/audio parts per turn, 120 seconds
queued PCM, 256 KiB public text per turn, 1 MiB scenario/filler, 2 MiB image,
1 GiB streamed evidence, maximum 24-hour run deadline. Session ledgers retain IDs
up to explicit lifetime caps. Usage objects are limited to 8 KiB. Summary transcript
previews are limited to 2,048 characters; full per-turn text stays in streamed JSONL.
Records are bounded and media deltas are omitted
from evidence; the runner never accumulates an entire conversation's PCM in RAM.
Choose output length/deadlines appropriate to the model. Run `cargo test --locked
--all-targets` and `cargo clippy --locked --all-targets -- -D warnings` before live
qualification.

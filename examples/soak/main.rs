//! Opt-in qualification client. All provider traffic goes through the public Session API.
mod player;
mod scenario;

use base64::{Engine, engine::general_purpose::STANDARD};
use melipona::{
    Command, Config, Continuation, Event, Session, Status, Tool, ToolCall, ToolCancellation,
    ToolExecutor, ToolFuture, ToolRegistry, ToolState,
};
use player::{FRAME, Player, RATE};
use scenario::{Result, Scenario, Turn};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufWriter, Read, Write},
    ops::ControlFlow,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

struct Echo {
    delay: Duration,
    receipts: mpsc::Sender<Value>,
}
impl ToolExecutor for Echo {
    fn execute(&self, call: ToolCall, _: ToolCancellation) -> ToolFuture<'_> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            self.receipts
                .try_send(json!({"call_id":call.call_id,"arguments":call.arguments}))
                .map_err(|_| "test receipt queue full".to_string())?;
            Ok(call.arguments)
        })
    }
}

struct Evidence {
    file: BufWriter<File>,
    start: Instant,
    bytes: usize,
    output_dir: PathBuf,
    audio_bytes: usize,
}
impl Evidence {
    fn write(&mut self, kind: &str, data: Value) -> Result<()> {
        let row = json!({"elapsed_ms":self.start.elapsed().as_secs_f64()*1000.0,"kind":kind,"data":scrub(data)});
        let bytes = serde_json::to_vec(&row)?;
        self.bytes += bytes.len() + 1;
        if self.bytes > 1024 * 1024 * 1024 {
            return Err("evidence exceeds 1 GiB run limit".into());
        }
        self.file.write_all(&bytes)?;
        self.file.write_all(b"\n")?;
        Ok(())
    }
}

fn scrub(mut value: Value) -> Value {
    match &mut value {
        Value::Object(map) => {
            for (key, value) in map {
                if ["audio", "delta", "image_url"].contains(&key.as_str())
                    && value.as_str().is_some_and(|s| s.len() > 4096)
                {
                    *value = json!({"omitted_bytes":value.as_str().unwrap().len()});
                } else if ["token", "api_key", "client_secret", "authorization"]
                    .contains(&key.to_ascii_lowercase().as_str())
                {
                    *value = json!("[redacted]");
                } else {
                    *value = scrub(std::mem::take(value));
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                *item = scrub(std::mem::take(item));
            }
        }
        _ => {}
    }
    value
}

fn fingerprint(data: &[u8]) -> String {
    let hash = data.iter().fold(0xcbf29ce484222325u64, |h, b| {
        (h ^ u64::from(*b)).wrapping_mul(0x100000001b3)
    });
    format!("fnv1a64:{hash:016x}")
}

fn binary_fingerprint() -> Result<String> {
    let mut file = File::open(std::env::current_exe()?)?;
    let mut bytes = [0u8; 8192];
    let mut hash = 0xcbf29ce484222325u64;
    loop {
        let count = file.read(&mut bytes)?;
        if count == 0 {
            break;
        }
        for byte in &bytes[..count] {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
        }
    }
    Ok(format!("fnv1a64:{hash:016x}"))
}

#[derive(Default, Serialize)]
struct Metrics {
    turn: usize,
    name: String,
    outcome: String,
    failures: Vec<String>,
    skips: Vec<String>,
    elapsed_ms: f64,
    input_end_ms: Option<f64>,
    first_text_ms: Option<f64>,
    first_audio_received_ms: Option<f64>,
    first_audio_rendered_ms: Option<f64>,
    end_input_to_first_text_ms: Option<f64>,
    end_input_to_first_audio_ms: Option<f64>,
    overlap_onset_ms: Option<f64>,
    overlap_clear_ms: Option<f64>,
    time_to_stop_ms: Option<f64>,
    rendered_audio_ms: u64,
    rendered_pcm: Option<String>,
    playback_gap_ms: u64,
    max_tick_lateness_ms: f64,
    output_words: usize,
    responses: usize,
    cancelled_responses: usize,
    tool_executions: usize,
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    last_usage: Option<Value>,
    input_tokens_source: Option<String>,
    reported_decode_tokens_per_second: Option<f64>,
    reported_peak_memory_bytes: Option<u64>,
    text_delivery_tokens_per_second: Option<f64>,
    context_chars_added: usize,
    context_refusal_code: Option<String>,
    transcript: String,
    transcript_truncated: bool,
}

struct Loaded {
    input: Vec<u8>,
    overlap: Vec<u8>,
    image: Option<String>,
}
fn load(turn: &Turn, base: &Path, evidence: &mut Evidence) -> Result<Loaded> {
    let mut read_pcm = |name: &Option<String>| -> Result<Vec<u8>> {
        if let Some(name) = name {
            let bytes = scenario::pcm(&base.join(name))?;
            evidence.write("fixture",json!({"name":name,"bytes":bytes.len(),"fingerprint":fingerprint(&bytes),"format":"pcm16le/24000/mono"}))?;
            Ok(bytes)
        } else {
            Ok(Vec::new())
        }
    };
    let input = read_pcm(&turn.pcm)?;
    let overlap = read_pcm(&turn.overlap.as_ref().and_then(|o| o.pcm.clone()))?;
    let image = if let Some(name) = &turn.image {
        let bytes = scenario::bounded_read(&base.join(name), 2 * 1024 * 1024)?;
        let mime = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
            "image/png"
        } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
            "image/jpeg"
        } else {
            return Err("image must be PNG/JPEG".into());
        };
        evidence.write("fixture",json!({"name":name,"bytes":bytes.len(),"fingerprint":fingerprint(&bytes),"format":mime}))?;
        Some(format!("data:{mime};base64,{}", STANDARD.encode(bytes)))
    } else {
        None
    };
    Ok(Loaded {
        input,
        overlap,
        image,
    })
}

fn send(session: &Session, command: Command) -> Result<()> {
    session.handle.send(command)?;
    Ok(())
}
fn extension(session: &Session, event: Value) -> Result<()> {
    send(session, Command::Frankie { event })
}
fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

/// Per-response bookkeeping for completion and transcript assertions.
#[derive(Default)]
struct Reply {
    /// Announced by `response.created` or `response.done`, not merely stopped locally.
    known: bool,
    done: bool,
    calls_tools: bool,
    voice_epoch: u64,
    delivered: bool,
    cleared: bool,
}

/// State for one scenario turn. `run_turn` owns the clocks; each event source has one method.
struct TurnRun<'a> {
    session: &'a mut Session,
    spec: &'a Scenario,
    turn: &'a Turn,
    index: usize,
    evidence: &'a mut Evidence,
    start: Instant,
    m: Metrics,
    input: Vec<u8>,
    input_at: usize,
    input_done: bool,
    overlap: Vec<u8>,
    overlap_at: usize,
    overlap_done: bool,
    overlap_response: Option<String>,
    recording: Option<BufWriter<File>>,
    player: Player,
    replies: HashMap<String, Reply>,
    text: Vec<(String, String)>,
    public_text_bytes: usize,
    voice_epoch: u64,
    user_speaking: bool,
    voice_reply_needed: bool,
    executed: HashMap<String, Value>,
    admitted: HashSet<String>,
    accepted: HashSet<String>,
    pending_truncations: HashSet<String>,
    last_activity: Instant,
    last_text_ms: Option<f64>,
    reported_text_tokens: Option<u64>,
    ticks: u64,
}

async fn run_turn(
    session: &mut Session,
    receipts: &mut mpsc::Receiver<Value>,
    spec: &Scenario,
    turn: &Turn,
    index: usize,
    base: &Path,
    evidence: &mut Evidence,
) -> Result<Metrics> {
    let mut run = TurnRun::start(session, spec, turn, index, base, evidence)?;
    let mut tick = tokio::time::interval(Duration::from_millis(32));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // The first rendered/input frame takes one frame duration.
    let deadline = tokio::time::sleep(spec.timeout());
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => {
                run.m.failures.push("turn deadline exceeded".into());
                break;
            }
            Some(receipt) = receipts.recv() => run.receipt(receipt)?,
            scheduled = tick.tick() => run.tick(scheduled.elapsed())?,
            event = run.session.events.recv() => {
                let event = event.ok_or("session event stream closed during turn")?;
                if run.event(event)?.is_break() {
                    break;
                }
            }
            changed = run.session.status.changed() => {
                changed.map_err(|_| "session status closed")?;
                match run.session.status.borrow().clone() {
                    Status::Failed(e) => return Err(e.into()),
                    Status::Closed => return Err("session closed".into()),
                    _ => {}
                }
            }
        }
        if run.m.context_refusal_code.is_some() || run.settled()? {
            break;
        }
    }
    run.finish()
}

impl<'a> TurnRun<'a> {
    fn start(
        session: &'a mut Session,
        spec: &'a Scenario,
        turn: &'a Turn,
        index: usize,
        base: &Path,
        evidence: &'a mut Evidence,
    ) -> Result<Self> {
        let loaded = load(turn, base, evidence)?;
        if turn.pcm.is_some() && !turn.server_vad {
            send(session, Command::ClearAudio)?;
        }
        let start = Instant::now();
        let context_chars = if index == 0 {
            0
        } else {
            spec.context.chars_per_turn
        };
        let filler = scenario::filler(spec.seed, index, context_chars);
        if !filler.is_empty() {
            let text = format!(
                "Reference data for this session; no reply needed to this data alone:\n{filler}"
            );
            send(session, Command::Text { text })?;
        }
        if let Some(image_url) = loaded.image {
            let text = turn.text.clone();
            send(session, Command::Image { image_url, text })?;
        } else if let Some(text) = &turn.text {
            send(session, Command::Text { text: text.clone() })?;
        }
        let mut m = Metrics {
            turn: index,
            name: turn.name.clone(),
            outcome: "pass".into(),
            context_chars_added: context_chars,
            ..Default::default()
        };
        let input_done = loaded.input.is_empty();
        if input_done {
            send(session, Command::Respond)?;
            m.input_end_ms = Some(ms(start));
        }
        let recording = if spec.record_audio {
            let name = format!("turn-{index:04}.pcm");
            let file = File::create(evidence.output_dir.join(&name))?;
            m.rendered_pcm = Some(name);
            Some(BufWriter::new(file))
        } else {
            None
        };
        evidence.write(
            "turn_started",
            json!({"turn":index,"name":turn.name,"context_chars_added":context_chars}),
        )?;
        Ok(Self {
            session,
            spec,
            turn,
            index,
            evidence,
            start,
            m,
            input: loaded.input,
            input_at: 0,
            input_done,
            overlap: loaded.overlap,
            overlap_at: 0,
            overlap_done: turn.overlap.is_none(),
            overlap_response: None,
            recording,
            player: Player::default(),
            replies: HashMap::new(),
            text: Vec::new(),
            public_text_bytes: 0,
            voice_epoch: 0,
            user_speaking: false,
            voice_reply_needed: turn.server_vad,
            executed: HashMap::new(),
            admitted: HashSet::new(),
            accepted: HashSet::new(),
            pending_truncations: HashSet::new(),
            last_activity: Instant::now(),
            last_text_ms: None,
            reported_text_tokens: None,
            ticks: 0,
        })
    }

    fn receipt(&mut self, receipt: Value) -> Result<()> {
        let id = receipt["call_id"].as_str().ok_or("invalid tool receipt")?;
        if self
            .executed
            .insert(id.to_owned(), receipt["arguments"].clone())
            .is_some()
        {
            self.m.failures.push("tool executed twice".into());
        }
        self.evidence.write("executor_receipt", receipt)?;
        self.last_activity = Instant::now();
        Ok(())
    }

    /// Stops local playback of `response`, reports what was heard, and records stop timing.
    fn stop(&mut self, response: &str, explicit: bool) -> Result<()> {
        self.replies.entry(response.into()).or_default().cleared = true;
        let changed = self.player.clear(response);
        if changed || explicit {
            let heard = self.player.heard(response);
            self.pending_truncations
                .extend(heard.iter().map(|h| h.item_id.clone()));
            self.evidence.write(
                "playback_stopped",
                json!({"response_id":response,"heard":heard,"explicit":explicit}),
            )?;
            let response_id = response.to_owned();
            send(
                self.session,
                if explicit {
                    Command::Interrupt { response_id, heard }
                } else {
                    Command::PlaybackStopped { response_id, heard }
                },
            )?;
        }
        self.last_activity = Instant::now();
        if self.overlap_response.as_deref() == Some(response) && self.m.overlap_clear_ms.is_none() {
            self.m.overlap_clear_ms = Some(ms(self.start));
        }
        Ok(())
    }

    /// One 32 ms frame: honor invalidations, maybe start overlap, render, then send the mic frame.
    fn tick(&mut self, lateness: Duration) -> Result<()> {
        self.ticks += 1;
        self.m.max_tick_lateness_ms = self
            .m
            .max_tick_lateness_ms
            .max(lateness.as_secs_f64() * 1000.0);
        let invalidated = self.player.invalidated(&self.session.playback.borrow());
        for response in invalidated {
            self.stop(&response, false)?;
        }
        self.maybe_start_overlap()?;
        let played = self.render()?;
        if self.spec.frankie {
            for event in self.player.feedback(self.ticks.is_multiple_of(8)) {
                extension(self.session, event)?;
            }
        }
        self.send_microphone(played)
    }

    fn maybe_start_overlap(&mut self) -> Result<()> {
        let Some(overlap) = &self.turn.overlap else {
            return Ok(());
        };
        if self.overlap_response.is_some()
            || self.player.rendered * 1000 / RATE < overlap.after_played_ms
        {
            return Ok(());
        }
        let Some(response) = self.player.active().map(str::to_owned) else {
            return Ok(());
        };
        self.overlap_response = Some(response.clone());
        if overlap.expect_clear && !self.overlap.is_empty() {
            self.voice_reply_needed = true;
        }
        self.m.overlap_onset_ms = Some(ms(self.start));
        self.evidence.write(
            "overlap_started",
            json!({"turn":self.index,"response_id":response,"explicit_interrupt":overlap.interrupt}),
        )?;
        if overlap.interrupt {
            self.stop(&response, true)?;
        }
        if self.overlap.is_empty() {
            self.overlap_done = true;
        }
        Ok(())
    }

    fn render(&mut self) -> Result<Vec<u8>> {
        let before = self.player.rendered;
        let played = self.player.render();
        if self.player.rendered > 0
            && let Some(file) = self.recording.as_mut()
        {
            self.evidence.audio_bytes += played.len();
            if self.evidence.audio_bytes > 1024 * 1024 * 1024 {
                return Err("rendered PCM exceeds 1 GiB run limit".into());
            }
            file.write_all(&played)?;
        }
        if self.player.rendered > before {
            self.last_activity = Instant::now();
            self.m.first_audio_rendered_ms.get_or_insert(ms(self.start));
        }
        Ok(played)
    }

    /// Input audio first, then overlap audio once overlap starts, otherwise silence.
    fn send_microphone(&mut self, played: Vec<u8>) -> Result<()> {
        let mut mic = vec![0; FRAME * 2];
        let mut input_end = false;
        if !self.input_done {
            input_end = feed(&mut mic, &self.input, &mut self.input_at);
            if input_end {
                self.input_done = true;
                self.m.input_end_ms = Some(ms(self.start));
            }
        } else if self.overlap_response.is_some() && !self.overlap_done {
            self.overlap_done = feed(&mut mic, &self.overlap, &mut self.overlap_at);
            if self.overlap_done {
                self.evidence
                    .write("overlap_ended", json!({"turn":self.index}))?;
            }
        }
        // Manual input must not accumulate silent audio while an unrelated
        // response runs. Continuous clocks belong to VAD/overlap scenarios.
        if !self.input_done || input_end || self.turn.server_vad || self.turn.overlap.is_some() {
            if self.spec.frankie {
                extension(
                    self.session,
                    json!({"type":"input_audio_buffer.append","audio":STANDARD.encode(&mic),"playback":STANDARD.encode(played)}),
                )?;
            } else {
                let audio = STANDARD.encode(mic);
                send(self.session, Command::Audio { audio })?;
            }
        }
        if input_end {
            self.evidence.write(
                "input_ended",
                json!({"turn":self.index,"elapsed_ms":self.m.input_end_ms}),
            )?;
            if !self.turn.server_vad {
                send(self.session, Command::CommitAudio)?;
                send(self.session, Command::Respond)?;
            }
        }
        Ok(())
    }

    fn event(&mut self, event: Event) -> Result<ControlFlow<()>> {
        match event {
            Event::CommandRejected { error } => {
                Err(format!("harness rejected command: {error}").into())
            }
            Event::PlaybackClear { response_id } => {
                self.stop(&response_id, false)?;
                Ok(ControlFlow::Continue(()))
            }
            Event::Tool { call_id, state } => {
                if state == ToolState::Admitted {
                    self.admitted.insert(call_id.clone());
                }
                self.evidence
                    .write("tool", json!({"call_id":call_id,"state":state}))?;
                Ok(ControlFlow::Continue(()))
            }
            Event::Server { event } => self.server(event),
        }
    }

    fn server(&mut self, event: Value) -> Result<ControlFlow<()>> {
        let kind = event["type"].as_str().unwrap_or("");
        if kind == "input_audio_buffer.committed"
            || kind.starts_with("response.")
            || kind.starts_with("conversation.item.")
        {
            self.last_activity = Instant::now();
        }
        if self
            .spec
            .telemetry
            .as_ref()
            .is_some_and(|t| t.event == kind)
        {
            self.telemetry(&event);
        }
        match kind {
            "error" => {
                if let Some(reason) = self.spec.context.refusal(&event["error"]) {
                    self.m.context_refusal_code = Some(reason);
                } else {
                    let code = event
                        .pointer("/error/code")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown");
                    self.m.failures.push(format!("provider error code: {code}"));
                }
                self.evidence.write("server", event)?;
                return Ok(ControlFlow::Break(()));
            }
            "conversation.item.truncated" => {
                if let Some(item) = event["item_id"].as_str() {
                    self.pending_truncations.remove(item);
                }
            }
            "input_audio_buffer.speech_started" => {
                self.user_speaking = true;
                self.voice_epoch += 1;
                if self.turn.server_vad
                    || self.turn.overlap.as_ref().is_some_and(|o| o.expect_clear)
                {
                    self.voice_reply_needed = true;
                }
            }
            "input_audio_buffer.speech_stopped" => self.user_speaking = false,
            "response.created" => {
                let id = event["response"]["id"]
                    .as_str()
                    .ok_or("missing response ID")?;
                let reply = self.replies.entry(id.into()).or_default();
                reply.known = true;
                reply.done = false;
                reply.calls_tools = false;
                reply.voice_epoch = self.voice_epoch;
            }
            "response.output_audio.done" => {
                if let Some(item) = event["item_id"].as_str() {
                    let part = event["content_index"].as_u64().unwrap_or(0) as u32;
                    self.player.part_done(item, part);
                }
            }
            "response.output_audio.delta" => {
                self.m.first_audio_received_ms.get_or_insert(ms(self.start));
                self.player.add(&event)?;
            }
            "response.output_text.delta" | "response.output_audio_transcript.delta" => {
                self.public_text(kind, &event)?;
            }
            "conversation.item.created" | "conversation.item.added" => {
                if event["item"]["type"] == "function_call_output"
                    && let Some(id) = event["item"]["call_id"].as_str()
                {
                    self.accepted.insert(id.to_owned());
                }
            }
            "frankie.playback.clear" => {
                if let Some(response) = event["response_id"].as_str() {
                    self.stop(response, false)?;
                }
            }
            "frankie.playback.pause" => self.m.failures.push(
                "provider paused playback; this runner qualifies uninterrupted backchannels".into(),
            ),
            "response.done" => self.response_done(&event["response"])?,
            _ => {}
        }
        let audio_delta = kind == "response.output_audio.delta";
        let mut logged = event;
        if audio_delta {
            let bytes = logged["delta"].as_str().map_or(0, str::len);
            logged["delta"] = json!({"base64_bytes":bytes});
        }
        self.evidence.write("server", logged)?;
        Ok(ControlFlow::Continue(()))
    }

    /// Reads only the provider counters a scenario explicitly maps.
    fn telemetry(&mut self, event: &Value) {
        let Some(mapping) = &self.spec.telemetry else {
            return;
        };
        let m = &mut self.m;
        if !mapping.input_tokens.is_empty() {
            m.input_tokens = mapping
                .input_tokens
                .iter()
                .try_fold(0u64, |sum, p| sum.checked_add(event.pointer(p)?.as_u64()?));
            if m.input_tokens.is_some() {
                m.input_tokens_source = Some(format!(
                    "{}:{}",
                    mapping.event,
                    mapping.input_tokens.join(" + ")
                ));
            }
        }
        let number = |path: &Option<String>| path.as_ref().and_then(|p| event.pointer(p));
        m.cached_input_tokens = number(&mapping.cached_input_tokens).and_then(Value::as_u64);
        m.reported_decode_tokens_per_second = number(&mapping.decode_tokens_per_second)
            .and_then(Value::as_f64)
            .filter(|v| v.is_finite() && *v >= 0.0);
        m.reported_peak_memory_bytes = number(&mapping.peak_memory_bytes).and_then(Value::as_u64);
    }

    fn public_text(&mut self, kind: &str, event: &Value) -> Result<()> {
        let delta = event["delta"].as_str().unwrap_or("");
        self.public_text_bytes += delta.len();
        if self.public_text_bytes > 256 * 1024 {
            return Err("turn public text exceeds 256 KiB".into());
        }
        let id = event["response_id"]
            .as_str()
            .ok_or("public text lacks response ID")?;
        match self.text.iter_mut().find(|(response, _)| response == id) {
            Some((_, text)) => text.push_str(delta),
            None => self.text.push((id.into(), delta.into())),
        }
        self.m.first_text_ms.get_or_insert(ms(self.start));
        if kind == "response.output_text.delta" {
            self.last_text_ms = Some(ms(self.start));
        }
        Ok(())
    }

    fn response_done(&mut self, response: &Value) -> Result<()> {
        let id = response["id"]
            .as_str()
            .ok_or("missing completed response ID")?;
        let calls_tools = response["output"]
            .as_array()
            .is_some_and(|items| items.iter().any(|i| i["type"] == "function_call"));
        self.player.done(id);
        let context = &self.spec.context;
        self.m.context_refusal_code = context
            .refusal(&response["status_details"]["error"])
            .or_else(|| context.refusal(&response["status_details"]));
        let reply = self.replies.entry(id.into()).or_default();
        reply.known = true;
        reply.done = true;
        reply.calls_tools = calls_tools;
        match response["status"].as_str() {
            Some(status @ ("completed" | "incomplete")) => {
                reply.delivered = true;
                if status == "incomplete" {
                    self.m.failures.push("response status: incomplete".into());
                }
                // A plain reply to the latest user speech satisfies it; the overlapped reply cannot.
                if !calls_tools
                    && reply.voice_epoch == self.voice_epoch
                    && self.overlap_response.as_deref() != Some(id)
                {
                    self.voice_reply_needed = false;
                }
            }
            Some("cancelled") if self.turn.overlap.is_some() || self.turn.server_vad => {
                self.m.cancelled_responses += 1;
            }
            _ if self.m.context_refusal_code.is_some() => {}
            other => self
                .m
                .failures
                .push(format!("response status: {}", other.unwrap_or("missing"))),
        }
        let usage = &response["usage"];
        if usage.is_object() {
            if serde_json::to_vec(usage)?.len() > 8192 {
                return Err("usage object exceeds 8 KiB".into());
            }
            if let Some(count) = usage["input_tokens"].as_u64() {
                self.m.input_tokens = Some(count);
                self.m.input_tokens_source = Some("response.done:usage.input_tokens".into());
            }
            if let Some(count) = usage
                .pointer("/input_token_details/cached_tokens")
                .and_then(Value::as_u64)
            {
                self.m.cached_input_tokens = Some(count);
            }
            self.reported_text_tokens = usage
                .pointer("/output_token_details/text_tokens")
                .and_then(Value::as_u64);
            self.m.last_usage = Some(usage.clone());
        }
        Ok(())
    }

    /// True once every response, tool, playback, truncation, and voice obligation is quiet.
    fn settled(&self) -> Result<bool> {
        if self.replies.len() > 128 || self.executed.len() > 128 || self.admitted.len() > 128 {
            return Err("turn response/tool ledger exceeds 128".into());
        }
        let mut known = self.replies.values().filter(|r| r.known).peekable();
        let quiet = known.peek().is_some()
            && known.clone().all(|r| r.done)
            && known.any(|r| r.done && !r.calls_tools)
            && self.admitted.iter().all(|id| self.accepted.contains(id))
            && self.player.active().is_none()
            && self.player.bytes == 0
            && self.input_done
            && self.pending_truncations.is_empty()
            && !self.user_speaking
            && !self.voice_reply_needed;
        // A missing overlap trigger is a failure, not an infinite wait after the model fell silent.
        Ok(quiet
            && (self.overlap_done || self.overlap_response.is_none())
            && self.last_activity.elapsed() >= Duration::from_millis(self.spec.settle_ms))
    }

    fn finish(mut self) -> Result<Metrics> {
        if let Some(file) = self.recording.as_mut() {
            file.flush()?;
        }
        let mut m = std::mem::take(&mut self.m);
        m.elapsed_ms = ms(self.start);
        m.responses = self.replies.values().filter(|r| r.known).count();
        m.tool_executions = self.executed.len();
        m.rendered_audio_ms = self.player.rendered * 1000 / RATE;
        m.playback_gap_ms = self.player.gap_samples * 1000 / RATE;
        m.transcript = self.transcript();
        m.output_words = m.transcript.split_whitespace().count();
        let since = |a: Option<f64>, b: Option<f64>| a.zip(b).map(|(a, b)| a - b);
        m.end_input_to_first_text_ms = since(m.first_text_ms, m.input_end_ms);
        m.end_input_to_first_audio_ms = since(m.first_audio_rendered_ms, m.input_end_ms);
        m.time_to_stop_ms = since(m.overlap_clear_ms, m.overlap_onset_ms);
        self.skips(&mut m);
        if m.context_refusal_code.is_none() {
            self.assert_expectations(&mut m);
        }
        if !m.failures.is_empty() {
            m.outcome = "fail".into();
        } else if m.context_refusal_code.is_some() {
            m.outcome = "capacity_refused".into();
        }
        self.evidence
            .write("turn_finished", serde_json::to_value(&m)?)?;
        self.evidence.file.flush()?;
        if m.transcript.chars().count() > 2048 {
            m.transcript = m.transcript.chars().take(2048).collect();
            m.transcript_truncated = true;
        }
        Ok(m)
    }

    /// Delivered, uncleared text; interruption turns keep only replies to the latest speech.
    fn transcript(&self) -> String {
        let expect_clear = self.turn.overlap.as_ref().is_some_and(|o| o.expect_clear);
        self.text
            .iter()
            .filter(|(id, _)| {
                self.replies.get(id).is_some_and(|r| {
                    r.delivered
                        && !r.cleared
                        && (!expect_clear
                            || (self.overlap_response.as_ref() != Some(id)
                                && r.voice_epoch == self.voice_epoch))
                })
            })
            .map(|(_, text)| text.as_str())
            .collect::<Vec<_>>()
            .join(" ")
    }

    fn skips(&self, m: &mut Metrics) {
        if let Some((tokens, (last, first))) = self
            .reported_text_tokens
            .zip(self.last_text_ms.zip(m.first_text_ms))
            && last > first
            && self.replies.values().filter(|r| r.known).count() == 1
        {
            m.text_delivery_tokens_per_second = Some(tokens as f64 / ((last - first) / 1000.0));
        } else {
            m.skips.push(
                "client text delivery rate unavailable: no timed text deltas with reported text token usage"
                    .into(),
            );
        }
        if m.input_tokens.is_none() {
            m.skips
                .push("provider did not report input tokens; context occupancy is unknown".into());
        }
        if m.cached_input_tokens.is_none() {
            m.skips
                .push("provider did not report cached input tokens".into());
        }
    }

    fn assert_expectations(&self, m: &mut Metrics) {
        let expect = &self.turn.expect;
        let transcript = m.transcript.to_lowercase();
        for expected in &expect.contains {
            if !transcript.contains(&expected.to_lowercase()) {
                m.failures.push(format!("missing public text: {expected}"));
            }
        }
        if m.output_words < expect.min_words {
            m.failures.push(format!(
                "response collapse/short reply: {} words < {}",
                m.output_words, expect.min_words
            ));
        }
        let mut actual = self
            .executed
            .values()
            .map(|a| a["text"].as_str().unwrap_or("").to_owned())
            .collect::<Vec<_>>();
        actual.sort();
        let mut expected = expect.tool_texts.clone();
        expected.sort();
        if actual != expected {
            m.failures
                .push("executed tools differ from expected exact text multiset".into());
        }
        if let Some(overlap) = &self.turn.overlap {
            if m.overlap_onset_ms.is_none() {
                m.failures.push("overlap was never triggered".into());
            } else if m.overlap_clear_ms.is_some() != overlap.expect_clear {
                m.failures
                    .push("backchannel/interruption clear expectation failed".into());
            }
            if let Some(max) = expect.max_stop_ms
                && !m.time_to_stop_ms.is_some_and(|t| t <= max as f64)
            {
                m.failures
                    .push(format!("interruption did not stop within {max} ms"));
            }
        }
    }
}

/// Copies the next microphone frame from `source`; returns true when `source` is exhausted.
fn feed(mic: &mut [u8], source: &[u8], at: &mut usize) -> bool {
    let count = (source.len() - *at).min(mic.len());
    mic[..count].copy_from_slice(&source[*at..*at + count]);
    *at += count;
    *at == source.len()
}

fn config(spec: &Scenario) -> Result<Config> {
    let mut c = Config::new(std::env::var("REALTIME_URL").map_err(|_| "set REALTIME_URL")?);
    c.model = std::env::var("REALTIME_MODEL").ok();
    c.bearer_token = std::env::var("REALTIME_TOKEN").ok();
    c.session = if let Ok(s) = std::env::var("REALTIME_SESSION") {
        serde_json::from_str(&s)?
    } else if spec.session.is_object() {
        spec.session.clone()
    } else {
        json!({"type":"realtime"})
    };
    c.frankie_extensions = spec.frankie;
    c.limits.responses = 32_768;
    c.limits.audio_parts = 32_768;
    c.limits.calls = 16_384;
    c.limits.tool_timeout = Duration::from_millis(spec.echo_delay_ms + 30_000);
    c.continuation = match std::env::var("REALTIME_TOOL_CONTINUATION").as_deref() {
        Ok("server") => Continuation::Server,
        Ok("client") | Err(_) => Continuation::Client,
        _ => return Err("invalid REALTIME_TOOL_CONTINUATION".into()),
    };
    Ok(c)
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 2 || args[0] == "--help" {
        eprintln!(
            "Usage: cargo run --locked --example soak -- SCENARIO.json NEW_OUTPUT_DIRECTORY\nSet REALTIME_URL; optional REALTIME_TOKEN, REALTIME_MODEL, REALTIME_SESSION, REALTIME_TOOL_CONTINUATION.\nNo audio device or inference subprocess is started. See scenarios/README.md."
        );
        return Ok(());
    }
    let path = PathBuf::from(&args[0]);
    let output = PathBuf::from(&args[1]);
    let spec = Scenario::load(&path)?;
    fs::create_dir(&output)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&output, fs::Permissions::from_mode(0o700))?;
    }
    let mut evidence = Evidence {
        file: BufWriter::new(File::create(output.join("events.jsonl"))?),
        start: Instant::now(),
        bytes: 0,
        output_dir: output.clone(),
        audio_bytes: 0,
    };
    let cfg = config(&spec)?;
    evidence.write("provenance",json!({"scenario":spec,"resolved_session":cfg.session,"crate_version":env!("CARGO_PKG_VERSION"),"binary_fingerprint":binary_fingerprint()?,
        "model":cfg.model,"continuation":format!("{:?}",cfg.continuation),"playback":"virtual PCM24k sample clock, 32 ms frames",
        "fixture_fingerprint":"FNV-1a reproducibility checksum, not cryptographic integrity","memory_measurement":"optional explicitly mapped provider counters; no process inspection"}))?;
    let (tx, mut receipts) = mpsc::channel(128);
    let tools = ToolRegistry::new(
        vec![Tool {
            name: "echo".into(),
            description: "Return the supplied text unchanged. Use only when explicitly requested."
                .into(),
            parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}),
        }],
        Arc::new(Echo {
            delay: Duration::from_millis(spec.echo_delay_ms),
            receipts: tx,
        }),
    )?;
    let mut metrics = Vec::new();
    let mut end = "turns_completed".to_string();
    let run = async {
        let mut session = Session::connect(cfg, tools).await?;
        let outcome:Result<()>=async {
            let mut negotiated=false;
            while !negotiated {
                tokio::select! {
                    event=session.events.recv()=>if let Some(Event::Server {event})=event {
                    if event["type"] == "session.updated" {
                        negotiated = true;
                        if spec.frankie
                            && event
                                .pointer("/session/frankie/playback_feedback")
                                .and_then(Value::as_bool)
                                != Some(true)
                        {
                            return Err("SKIP: endpoint does not advertise Frankie playback feedback".into());
                        }
                        for direction in ["input", "output"] {
                            if let Some(format) = event.pointer(&format!("/session/audio/{direction}/format"))
                                && (format["type"] != "audio/pcm"
                                    || format["rate"].as_u64().unwrap_or(24000) != 24000)
                            {
                                return Err(
                                    "SKIP: this sample-clock runner requires PCM16 input/output at 24 kHz".into(),
                                );
                            }
                        }
                    }
                    evidence.write("server", event)?;
                },
                    changed=session.status.changed()=>{
                    if changed.is_err() {
                        return Err("initialization closed".into());
                    }
                    match session.status.borrow().clone() {
                        Status::Failed(e) => return Err(e.into()),
                        Status::Closed => return Err("initialization closed".into()),
                        _ => {}
                    }
                }
                }
            }
            if let Some(text)=&spec.setup {send(&session,Command::Text {text:text.clone()})?;}
            for index in 0..spec.rounds*spec.turns.len() {
                let turn=&spec.turns[index%spec.turns.len()];
                let m=run_turn(&mut session,&mut receipts,&spec,turn,index,path.parent().unwrap_or(Path::new(".")),&mut evidence).await?;
                let failed=m.outcome=="fail";let capacity=m.context_refusal_code.is_some();
                let target=spec.context.target_input_tokens.is_some_and(|n|m.input_tokens.is_some_and(|actual|actual>=n));
                metrics.push(m);
                if failed {end="assertion_failed".into();break;}
                if capacity {end=if index==0 {"capacity_refused_at_start"}else{"capacity_refused"}.into();break;}
                if target {end="reported_context_target_reached".into();break;}
            }
            Ok(())
        }.await;
        let finish = session.finish().await;
        outcome?;
        finish?;
        Ok(())
    };
    let result: Result<()> =
        match tokio::time::timeout(Duration::from_millis(spec.run_timeout_ms), run).await {
            Ok(result) => result,
            Err(_) => Err("run deadline exceeded".into()),
        };
    if let Err(error) = &result {
        end = format!("run_error: {error}");
        evidence.write("run_error", json!({"error":error.to_string()}))?;
    }
    if end == "turns_completed" && spec.context.target_input_tokens.is_some() {
        end = "context_target_not_verified".into();
    }
    let passed = matches!(
        end.as_str(),
        "turns_completed" | "reported_context_target_reached" | "capacity_refused"
    );
    let summary = json!({"name":spec.name,"outcome":if passed {"pass"}else if end.contains("SKIP:") {"skip"}else{"fail"},"end":end,
        "elapsed_ms":evidence.start.elapsed().as_secs_f64()*1000.0,"turns_completed":metrics.len(),"turns":metrics,
        "limitations":["Virtual playback timings include this client/network/provider, not a physical microphone or speaker.",
            "Text delivery rate is not a GPU decode benchmark. Audio transcripts do not expose brain token timing.",
            "Missing context/cache usage remains unknown; ASCII characters are not tokenizer counts.",
            "Keyword/tool assertions are reproducible regressions, not intelligence or natural voice quality scores."]});
    evidence.write("summary", summary.clone())?;
    evidence.file.flush()?;
    fs::write(
        output.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!(
        "{}",
        json!({"outcome":summary["outcome"],"end":end,"turns_completed":metrics.len()})
    );
    if !passed {
        return Err("qualification did not pass; inspect summary.json".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;

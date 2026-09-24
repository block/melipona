//! Opt-in qualification client. All provider traffic goes through the public Session API.
mod player;
mod scenario;

use base64::{Engine, engine::general_purpose::STANDARD};
use melipona::{
    Command, Config, Continuation, Event, Session, Status, Tool, ToolCall, ToolCancellation,
    ToolExecutor, ToolFuture, ToolRegistry,
};
use player::{FRAME, Player, RATE};
use scenario::{Result, Scenario, Turn};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::{BufWriter, Read, Write},
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

fn clear(
    session: &Session,
    player: &mut Player,
    response: &str,
    explicit: bool,
    evidence: &mut Evidence,
) -> Result<Vec<String>> {
    let changed = player.clear(response);
    let mut pending = Vec::new();
    if changed || explicit {
        let heard = player.heard(response);
        pending = heard.iter().map(|h| h.item_id.clone()).collect();
        evidence.write(
            "playback_stopped",
            json!({"response_id":response,"heard":heard,"explicit":explicit}),
        )?;
        send(
            session,
            if explicit {
                Command::Interrupt {
                    response_id: response.into(),
                    heard,
                }
            } else {
                Command::PlaybackStopped {
                    response_id: response.into(),
                    heard,
                }
            },
        )?;
    }
    Ok(pending)
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
    let loaded = load(turn, base, evidence)?;
    if turn.pcm.is_some() && !turn.server_vad {
        send(session, Command::ClearAudio)?;
    }
    let start = Instant::now();
    let mut m = Metrics {
        turn: index,
        name: turn.name.clone(),
        outcome: "pass".into(),
        ..Default::default()
    };
    let context_chars = if index == 0 {
        0
    } else {
        spec.context.chars_per_turn
    };
    m.context_chars_added = context_chars;
    let filler = scenario::filler(spec.seed, index, context_chars);
    if !filler.is_empty() {
        send(
            session,
            Command::Text {
                text: format!(
                    "Reference data for this session; no reply needed to this data alone:\n{filler}"
                ),
            },
        )?;
    }
    if let Some(image_url) = loaded.image {
        send(
            session,
            Command::Image {
                image_url,
                text: turn.text.clone(),
            },
        )?;
    } else if let Some(text) = &turn.text {
        send(session, Command::Text { text: text.clone() })?;
    }
    let mut input_at = 0usize;
    let mut input_done = loaded.input.is_empty();
    if input_done {
        send(session, Command::Respond)?;
        m.input_end_ms = Some(ms(start));
    }
    let mut recording = if spec.record_audio {
        let name = format!("turn-{index:04}.pcm");
        m.rendered_pcm = Some(name.clone());
        Some(BufWriter::new(File::create(
            evidence.output_dir.join(name),
        )?))
    } else {
        None
    };
    let mut player = Player::default();
    let mut responses = HashMap::<String, (bool, bool)>::new(); // done, contains function calls
    let mut response_text = HashMap::<String, String>::new();
    let mut response_order = Vec::<String>::new();
    let mut delivered_responses = HashSet::<String>::new();
    let mut cleared_responses = HashSet::<String>::new();
    let mut public_text_bytes = 0usize;
    let mut voice_epoch = 0u64;
    let mut user_speaking = false;
    let mut voice_reply_needed = turn.server_vad;
    let mut response_voice_epoch = HashMap::<String, u64>::new();
    let mut tools = HashMap::<String, Value>::new();
    let mut accepted = HashSet::new();
    let mut pending_truncations = HashSet::<String>::new();
    let mut tool_ids = HashSet::new();
    let mut overlap_at = 0usize;
    let mut overlap_response = None::<String>;
    let mut overlap_done = turn.overlap.is_none();
    let mut last_activity = Instant::now();
    let mut last_text_ms = None;
    let mut reported_text_tokens = None;
    let mut ticks = 0u64;
    let mut tick = tokio::time::interval(Duration::from_millis(32));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tick.tick().await; // The first rendered/input frame takes one frame duration.
    let deadline = tokio::time::sleep(spec.timeout());
    tokio::pin!(deadline);
    evidence.write(
        "turn_started",
        json!({"turn":index,"name":turn.name,"context_chars_added":context_chars}),
    )?;
    loop {
        tokio::select! {
            _=&mut deadline=>{
                    m.failures.push("turn deadline exceeded".into());
                    break;
                }
            receipt=receipts.recv()=>if let Some(receipt)=receipt {
                    let id = receipt["call_id"]
                        .as_str()
                        .ok_or("invalid tool receipt")?
                        .to_owned();
                    if tools.insert(id, receipt["arguments"].clone()).is_some() {
                        m.failures.push("tool executed twice".into());
                    }
                    evidence.write("executor_receipt", receipt)?;
                    last_activity = Instant::now();
                },
            scheduled=tick.tick()=>{
                    ticks += 1;
                    m.max_tick_lateness_ms = m
                        .max_tick_lateness_ms
                        .max(scheduled.elapsed().as_secs_f64() * 1000.0);
                    for rid in player.invalidated(&session.playback.borrow()) {
                        cleared_responses.insert(rid.clone());
                        pending_truncations.extend(clear(session, &mut player, &rid, false, evidence)?);
                        last_activity = Instant::now();
                        if overlap_response.as_deref() == Some(&rid) && m.overlap_clear_ms.is_none() {
                            m.overlap_clear_ms = Some(ms(start));
                        }
                    }
                    if let Some(overlap) = &turn.overlap
                        && overlap_response.is_none()
                        && player.rendered * 1000 / RATE >= overlap.after_played_ms
                        && let Some(rid) = player.active().map(str::to_owned)
                    {
                        overlap_response = Some(rid.clone());
                        if overlap.expect_clear && !loaded.overlap.is_empty(){voice_reply_needed=true;}
                        m.overlap_onset_ms = Some(ms(start));
                        evidence.write(
                            "overlap_started",
                            json!({"turn":index,"response_id":rid,"explicit_interrupt":overlap.interrupt}),
                        )?;
                        if overlap.interrupt {
                            cleared_responses.insert(rid.clone());
                            pending_truncations.extend(clear(session, &mut player, &rid, true, evidence)?);
                            last_activity = Instant::now();
                            m.overlap_clear_ms = Some(ms(start));
                        }
                        if loaded.overlap.is_empty() {
                            overlap_done = true;
                        }
                    }
                    let before = player.rendered;
                    let played = player.render();
                    if player.rendered > 0
                        && let Some(file) = recording.as_mut()
                    {
                        evidence.audio_bytes += played.len();
                        if evidence.audio_bytes > 1024 * 1024 * 1024 {
                            return Err("rendered PCM exceeds 1 GiB run limit".into());
                        }
                        file.write_all(&played)?;
                    }
                    if player.rendered > before {
                        last_activity = Instant::now();
                        if m.first_audio_rendered_ms.is_none() {
                            m.first_audio_rendered_ms = Some(ms(start));
                        }
                    }
                    if spec.frankie {
                        for event in player.feedback(ticks.is_multiple_of(8)) {
                            extension(session, event)?;
                        }
                    }
                    let mut mic = vec![0; FRAME * 2];
                    let mut input_end = false;
                    if !input_done {
                        let count = (loaded.input.len() - input_at).min(mic.len());
                        mic[..count].copy_from_slice(&loaded.input[input_at..input_at + count]);
                        input_at += count;
                        if input_at == loaded.input.len() {
                            input_done = true;
                            input_end = true;
                            m.input_end_ms = Some(ms(start));
                        }
                    } else if overlap_response.is_some() && !overlap_done {
                        let count = (loaded.overlap.len() - overlap_at).min(mic.len());
                        mic[..count].copy_from_slice(&loaded.overlap[overlap_at..overlap_at + count]);
                        overlap_at += count;
                        if overlap_at == loaded.overlap.len() {
                            overlap_done = true;
                            evidence.write("overlap_ended", json!({"turn":index}))?;
                        }
                    }
                    // Manual input must not accumulate silent audio while an unrelated
                    // response runs. Continuous clocks belong to VAD/overlap scenarios.
                    if !input_done || input_end || turn.server_vad || turn.overlap.is_some() {
                        if spec.frankie {
                            extension(session,json!({"type":"input_audio_buffer.append","audio":STANDARD.encode(&mic),"playback":STANDARD.encode(played)}))?;
                        } else {send(session,Command::Audio {audio:STANDARD.encode(mic)})?;}
                    }
                    if input_end {
                        evidence.write(
                            "input_ended",
                            json!({"turn":index,"elapsed_ms":m.input_end_ms}),
                        )?;
                        if !turn.server_vad {
                            send(session, Command::CommitAudio)?;
                            send(session, Command::Respond)?;
                        }
                    }
                },
            event=session.events.recv()=>{
                    let Some(event) = event else {
                        return Err("session event stream closed during turn".into());
                    };
                    match event {
                        Event::CommandRejected { error } => {
                            return Err(format!("harness rejected command: {error}").into());
                        }
                        Event::PlaybackClear { response_id } => {
                            cleared_responses.insert(response_id.clone());
                            pending_truncations.extend(clear(session, &mut player, &response_id, false, evidence)?);
                            last_activity = Instant::now();
                            if overlap_response.as_deref() == Some(&response_id) && m.overlap_clear_ms.is_none() {
                                m.overlap_clear_ms = Some(ms(start));
                            }
                        }
                        Event::Tool { call_id, state } => {
                            if state == "admitted" {
                                tool_ids.insert(call_id.clone());
                            }
                            evidence.write("tool", json!({"call_id":call_id,"state":state}))?;
                        }
                        Event::Server { event } => {
                            let kind = event["type"].as_str().unwrap_or("");
                            if kind == "input_audio_buffer.committed"
                                || kind.starts_with("response.")
                                || kind.starts_with("conversation.item.")
                            {
                                last_activity = Instant::now();
                            }
                            if let Some(mapping) = &spec.telemetry
                                && kind == mapping.event
                            {
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
                                m.cached_input_tokens = mapping
                                    .cached_input_tokens
                                    .as_ref()
                                    .and_then(|p| event.pointer(p)?.as_u64());
                                m.reported_decode_tokens_per_second = mapping
                                    .decode_tokens_per_second
                                    .as_ref()
                                    .and_then(|p| event.pointer(p)?.as_f64())
                                    .filter(|v| v.is_finite() && *v >= 0.0);
                                m.reported_peak_memory_bytes = mapping
                                    .peak_memory_bytes
                                    .as_ref()
                                    .and_then(|p| event.pointer(p)?.as_u64());
                            }
                            match kind {
                                "error" => {
                                    let code = event
                                        .pointer("/error/code")
                                        .and_then(Value::as_str)
                                        .unwrap_or("unknown");
                                    if let Some(reason) = spec.context.refusal(&event["error"]) {
                                        m.context_refusal_code = Some(reason);
                                    } else {
                                        m.failures.push(format!("provider error code: {code}"));
                                    }
                                    evidence.write("server", event)?;
                                    break;
                                }
                                "conversation.item.truncated" => {
                                    if let Some(item) = event["item_id"].as_str() {
                                        pending_truncations.remove(item);
                                    }
                                }
                                "input_audio_buffer.speech_started" => {
                                user_speaking=true;voice_epoch+=1;
                                if turn.server_vad || turn.overlap.as_ref().is_some_and(|o|o.expect_clear){voice_reply_needed=true;}
                            }
                            "input_audio_buffer.speech_stopped" => {user_speaking=false;}
                            "response.created" => {
                                    let rid = event["response"]["id"]
                                        .as_str()
                                        .ok_or("missing response ID")?;
                                    responses.insert(rid.into(), (false, false));
                                response_voice_epoch.insert(rid.into(),voice_epoch);
                                }
                                "response.output_audio.done" => {
                                    if let Some(item) = event["item_id"].as_str() {
                                        player.part_done(item, event["content_index"].as_u64().unwrap_or(0) as u32);
                                    }
                                }
                                "response.output_audio.delta" => {
                                    if m.first_audio_received_ms.is_none() {
                                        m.first_audio_received_ms = Some(ms(start));
                                    }
                                    player.add(&event)?;
                                }
                                "response.output_text.delta" | "response.output_audio_transcript.delta" => {
                                    let text = event["delta"].as_str().unwrap_or("");
                                    if public_text_bytes + text.len() > 256 * 1024 {
                                        return Err("turn public text exceeds 256 KiB".into());
                                    }
                                    public_text_bytes+=text.len();
                                let rid=event["response_id"].as_str().ok_or("public text lacks response ID")?;
                                if !response_text.contains_key(rid){response_order.push(rid.into());}
                                response_text.entry(rid.into()).or_default().push_str(text);
                                    if m.first_text_ms.is_none() {
                                        m.first_text_ms = Some(ms(start));
                                    }
                                    if kind == "response.output_text.delta" {
                                        last_text_ms = Some(ms(start));
                                    }
                                }
                                "conversation.item.created" | "conversation.item.added" => {
                                    if event["item"]["type"] == "function_call_output"
                                        && let Some(id) = event["item"]["call_id"].as_str()
                                    {
                                        accepted.insert(id.to_owned());
                                    }
                                }
                                "frankie.playback.clear" => {
                                    if let Some(rid) = event["response_id"].as_str() {
                                        cleared_responses.insert(rid.to_owned());
                                        pending_truncations.extend(clear(
                                            session,
                                            &mut player,
                                            rid,
                                            false,
                                            evidence,
                                        )?);
                                        last_activity = Instant::now();
                                        if overlap_response.as_deref() == Some(rid) && m.overlap_clear_ms.is_none()
                                        {
                                            m.overlap_clear_ms = Some(ms(start));
                                        }
                                    }
                                }
                                "frankie.playback.pause" => {
                                    m.failures.push("provider paused playback; this runner qualifies uninterrupted backchannels".into());
                                }
                                "response.done" => {
                                    let response = &event["response"];
                                    let rid = response["id"]
                                        .as_str()
                                        .ok_or("missing completed response ID")?;
                                    let calls = response["output"]
                                        .as_array()
                                        .is_some_and(|items| items.iter().any(|i| i["type"] == "function_call"));
                                    responses.insert(rid.into(), (true, calls));
                                    player.done(rid);
                                    m.context_refusal_code = spec
                                        .context
                                        .refusal(&response["status_details"]["error"])
                                        .or_else(|| spec.context.refusal(&response["status_details"]));
                                    match response["status"].as_str() {
                                        Some(status @ ("completed" | "incomplete")) => {
                                        delivered_responses.insert(rid.to_owned());
                                        if status == "incomplete" { m.failures.push("response status: incomplete".into()); }
                                        if !calls && response_voice_epoch.get(rid)==Some(&voice_epoch) && overlap_response.as_deref()!=Some(rid){voice_reply_needed=false;}
                                    }
                                        Some("cancelled") if turn.overlap.is_some() || turn.server_vad => {m.cancelled_responses+=1;}
                                        _ if m.context_refusal_code.is_some() => {}
                                        other => m
                                            .failures
                                            .push(format!("response status: {}", other.unwrap_or("missing"))),
                                    }
                                    if response["usage"].is_object() {
                                        let usage = &response["usage"];
                                        if let Some(count) = usage["input_tokens"].as_u64() {
                                            m.input_tokens = Some(count);
                                            m.input_tokens_source = Some("response.done:usage.input_tokens".into());
                                        }
                                        if let Some(count) = usage
                                            .pointer("/input_token_details/cached_tokens")
                                            .and_then(Value::as_u64)
                                        {
                                            m.cached_input_tokens = Some(count);
                                        }
                                        reported_text_tokens = usage
                                            .pointer("/output_token_details/text_tokens")
                                            .and_then(Value::as_u64);
                                        if serde_json::to_vec(usage)?.len()>8192 {return Err("usage object exceeds 8 KiB".into());}
                                    m.last_usage = Some(usage.clone());
                                    }
                                }
                                _ => {}
                            }
                            let audio_delta = kind == "response.output_audio.delta";
                            let mut logged = event;
                            if audio_delta {
                                let bytes = logged["delta"].as_str().map_or(0, str::len);
                                logged["delta"] = json!({"base64_bytes":bytes});
                            }
                            evidence.write("server", logged)?;
                        }
                    }
                },
            changed=session.status.changed()=>{
                    if changed.is_err() {
                        return Err("session status closed".into());
                    }
                    match session.status.borrow().clone() {
                        Status::Failed(e) => return Err(e.into()),
                        Status::Closed => return Err("session closed".into()),
                        _ => {}
                    }
                }
        }
        if m.context_refusal_code.is_some() {
            break;
        }
        if responses.len() > 128 || tools.len() > 128 || tool_ids.len() > 128 {
            return Err("turn response/tool ledger exceeds 128".into());
        }
        let finished = !responses.is_empty()
            && responses.values().all(|(done, _)| *done)
            && responses.values().any(|(done, calls)| *done && !*calls)
            && tool_ids.iter().all(|id| accepted.contains(id))
            && player.active().is_none()
            && player.bytes == 0
            && input_done
            && pending_truncations.is_empty()
            && !user_speaking
            && !voice_reply_needed;
        // A missing overlap trigger is a failure, not an infinite wait after the model fell silent.
        if finished
            && (overlap_done || overlap_response.is_none())
            && last_activity.elapsed() >= Duration::from_millis(spec.settle_ms)
        {
            break;
        }
    }
    if let Some(file) = recording.as_mut() {
        file.flush()?;
    }
    m.elapsed_ms = ms(start);
    m.responses = responses.len();
    m.tool_executions = tools.len();
    m.rendered_audio_ms = player.rendered * 1000 / RATE;
    m.playback_gap_ms = player.gap_samples * 1000 / RATE;
    m.transcript = response_order
        .iter()
        .filter(|rid| delivered_responses.contains(*rid) && !cleared_responses.contains(*rid))
        .filter(|rid| {
            !turn.overlap.as_ref().is_some_and(|o| o.expect_clear)
                || (overlap_response.as_ref() != Some(*rid)
                    && response_voice_epoch.get(*rid) == Some(&voice_epoch))
        })
        .filter_map(|rid| response_text.get(rid))
        .cloned()
        .collect::<Vec<_>>()
        .join(" ");
    m.output_words = m.transcript.split_whitespace().count();
    m.end_input_to_first_text_ms = m.first_text_ms.zip(m.input_end_ms).map(|(a, b)| a - b);
    m.end_input_to_first_audio_ms = m
        .first_audio_rendered_ms
        .zip(m.input_end_ms)
        .map(|(a, b)| a - b);
    m.time_to_stop_ms = m
        .overlap_clear_ms
        .zip(m.overlap_onset_ms)
        .map(|(a, b)| a - b);
    if let Some((tokens, (last, first))) =
        reported_text_tokens.zip(last_text_ms.zip(m.first_text_ms))
        && last > first
        && responses.len() == 1
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
    if m.context_refusal_code.is_none() {
        for expected in &turn.expect.contains {
            if !m
                .transcript
                .to_lowercase()
                .contains(&expected.to_lowercase())
            {
                m.failures.push(format!("missing public text: {expected}"));
            }
        }
        if m.output_words < turn.expect.min_words {
            m.failures.push(format!(
                "response collapse/short reply: {} words < {}",
                m.output_words, turn.expect.min_words
            ));
        }
        let mut actual = tools
            .values()
            .map(|a| a["text"].as_str().unwrap_or("").to_owned())
            .collect::<Vec<_>>();
        actual.sort();
        let mut expected = turn.expect.tool_texts.clone();
        expected.sort();
        if actual != expected {
            m.failures
                .push("executed tools differ from expected exact text multiset".into());
        }
        if let Some(o) = &turn.overlap {
            if m.overlap_onset_ms.is_none() {
                m.failures.push("overlap was never triggered".into());
            } else if m.overlap_clear_ms.is_some() != o.expect_clear {
                m.failures
                    .push("backchannel/interruption clear expectation failed".into());
            }
            if let Some(max) = turn.expect.max_stop_ms
                && !m.time_to_stop_ms.is_some_and(|t| t <= max as f64)
            {
                m.failures
                    .push(format!("interruption did not stop within {max} ms"));
            }
        }
    }
    if !m.failures.is_empty() {
        m.outcome = "fail".into();
    } else if m.context_refusal_code.is_some() {
        m.outcome = "capacity_refused".into();
    }
    evidence.write("turn_finished", serde_json::to_value(&m)?)?;
    evidence.file.flush()?;
    if m.transcript.chars().count() > 2048 {
        m.transcript = m.transcript.chars().take(2048).collect();
        m.transcript_truncated = true;
    }
    Ok(m)
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

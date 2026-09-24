//! Opt-in provider conformance probe for the GA Realtime WebSocket protocol.
//!
//! Every server event is checked against the reference's required fields. Each check
//! then drives one documented client flow on a fresh session through the public
//! Session API and asserts the documented server behavior. A provider conforms only
//! if every check passes; a skip names what the run could not establish.
mod schema;

use base64::{Engine, engine::general_purpose::STANDARD};
use melipona::{
    Command, Config, Event, Heard, Session, Tool, ToolCall, ToolCancellation, ToolExecutor,
    ToolFuture, ToolRegistry,
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::time::{Instant, timeout_at};

type Outcome = Result<(), Verdict>;
enum Verdict {
    Fail(String),
    Skip(String),
}
fn fail<T>(why: impl Into<String>) -> Result<T, Verdict> {
    Err(Verdict::Fail(why.into()))
}
fn check(ok: bool, why: &str) -> Outcome {
    if ok { Ok(()) } else { fail(why) }
}

const TEXT: &[&str] = &["text"];
/// The reference lists each of these as an item acknowledgement; the session accepts any.
const ITEM_ACK: &[&str] = &[
    "conversation.item.added",
    "conversation.item.created",
    "conversation.item.done",
];
const AUDIO: &[&str] = &["audio"];
const CHECKS: &[&str] = &[
    "session",
    "text_response",
    "cancel",
    "out_of_band",
    "error_event_id",
    "items",
    "tool_call",
    "audio_input",
    "audio_truncate",
];

struct Echo;
impl ToolExecutor for Echo {
    fn execute(&self, call: ToolCall, _: ToolCancellation) -> ToolFuture<'_> {
        Box::pin(async move { Ok(call.arguments) })
    }
}

fn registry(tools: bool) -> Result<ToolRegistry, Verdict> {
    if !tools {
        return Ok(ToolRegistry::empty());
    }
    let tool = Tool {
        name: "echo".into(),
        description: "Return the supplied text unchanged.".into(),
        parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}),
    };
    ToolRegistry::new(vec![tool], Arc::new(Echo)).map_err(|e| Verdict::Fail(e.to_string()))
}

/// One session plus every server event it produced, schema-checked on arrival.
struct Probe {
    session: Session,
    seen: Vec<Value>,
    deadline: Instant,
    unknown: BTreeSet<String>,
}

impl Probe {
    async fn open(modalities: &[&str], extra: Value, tools: bool) -> Result<Self, Verdict> {
        let url =
            std::env::var("REALTIME_URL").map_err(|_| Verdict::Fail("set REALTIME_URL".into()))?;
        let mut session = json!({"type":"realtime","output_modalities":modalities});
        if let (Some(session), Some(extra)) = (session.as_object_mut(), extra.as_object()) {
            session.extend(extra.clone());
        }
        // Single-session providers may still be releasing the previous check's session.
        let settle = Instant::now() + Duration::from_secs(30);
        let session = loop {
            let mut config = Config::new(url.clone());
            config.model = std::env::var("REALTIME_MODEL").ok();
            config.bearer_token = std::env::var("REALTIME_TOKEN").ok();
            config.limits.initialize_timeout = Duration::from_secs(60);
            config.session = session.clone();
            match Session::connect(config, registry(tools)?).await {
                Ok(session) => break session,
                Err(e) if Instant::now() >= settle => return fail(format!("connect: {e}")),
                Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
            }
        };
        let mut probe = Probe {
            session,
            seen: Vec::new(),
            deadline: Instant::now() + Duration::from_secs(120),
            unknown: BTreeSet::new(),
        };
        probe.until("session.updated").await?;
        Ok(probe)
    }

    fn send(&self, command: Command) -> Outcome {
        self.session
            .handle
            .send(command)
            .map_err(|e| Verdict::Fail(format!("send: {e}")))
    }

    /// Next server event, after checking its required fields.
    async fn next(&mut self) -> Result<Value, Verdict> {
        loop {
            let event = timeout_at(self.deadline, self.session.events.recv())
                .await
                .map_err(|_| Verdict::Fail(format!("timed out; last event: {}", self.last())))?;
            let event = match event {
                Some(Event::Server { event }) => event,
                Some(Event::CommandRejected { error }) => {
                    return fail(format!("rejected: {error}"));
                }
                Some(Event::PlaybackClear { .. } | Event::Tool { .. }) => continue,
                None => {
                    let status = self.session.status.borrow().clone();
                    return fail(format!("session ended: {status:?}"));
                }
            };
            self.required(&event)?;
            self.seen.push(event.clone());
            return Ok(event);
        }
    }

    async fn until(&mut self, kind: &str) -> Result<Value, Verdict> {
        self.until_any(&[kind]).await
    }

    async fn until_any(&mut self, kinds: &[&str]) -> Result<Value, Verdict> {
        loop {
            let event = self.next().await?;
            if kinds.iter().any(|kind| event["type"] == *kind) {
                return Ok(event);
            }
            if event["type"] == "error" {
                return fail(format!("unexpected error: {}", event["error"]));
            }
        }
    }

    /// The acknowledgement of the item `matches` accepts.
    async fn item(&mut self, matches: impl Fn(&Value) -> bool) -> Result<Value, Verdict> {
        loop {
            let event = self.until_any(ITEM_ACK).await?;
            if matches(&event["item"]) {
                return Ok(event);
            }
        }
    }

    fn required(&mut self, event: &Value) -> Outcome {
        let kind = event["type"].as_str().unwrap_or("");
        let Some((_, spec)) = schema::REQUIRED.iter().find(|(name, _)| *name == kind) else {
            self.unknown.insert(kind.to_owned());
            return Ok(());
        };
        for field in spec.split_whitespace() {
            let (name, kind_code) = field.split_once(':').expect("generated table");
            let value = &event[name];
            let ok = match kind_code {
                "s" => value.is_string(),
                "n" => value.is_number(),
                "o" => value.is_object(),
                _ => value.is_array(),
            };
            if !ok {
                return fail(format!("{kind} lacks required {name}"));
            }
        }
        Ok(())
    }

    fn last(&self) -> String {
        self.seen
            .last()
            .and_then(|e| e["type"].as_str())
            .unwrap_or("none")
            .to_owned()
    }

    /// Events seen since `from` for one response, by type.
    fn kinds(&self, from: usize, response: &str) -> Vec<&str> {
        self.seen[from..]
            .iter()
            .filter(|e| e["response_id"] == response || e["response"]["id"] == response)
            .filter_map(|e| e["type"].as_str())
            .collect()
    }

    async fn finish(self) -> BTreeSet<String> {
        let _ = self.session.finish().await;
        self.unknown
    }
}

fn text(request: &str) -> Command {
    Command::Text {
        text: request.into(),
    }
}
fn respond(parameters: Value) -> Command {
    Command::Respond {
        response: Some(parameters),
    }
}
fn event(event: Value) -> Command {
    Command::Event { event }
}

/// `expected` must occur in order, allowing any events in between.
fn in_order(kinds: &[&str], expected: &[&str]) -> Outcome {
    let mut rest = kinds.iter();
    for want in expected {
        if !rest.any(|kind| kind == want) {
            return fail(format!("missing or out of order: {want}; saw {kinds:?}"));
        }
    }
    Ok(())
}

fn transcript(probe: &Probe, from: usize, response: &str) -> String {
    probe.seen[from..]
        .iter()
        .filter(|e| e["response_id"] == response)
        .filter(|e| {
            e["type"] == "response.output_text.delta"
                || e["type"] == "response.output_audio_transcript.delta"
        })
        .filter_map(|e| e["delta"].as_str())
        .collect()
}

async fn run(name: &str) -> (Outcome, BTreeSet<String>) {
    let probe = match name {
        "audio_input" => {
            Probe::open(
                TEXT,
                json!({"audio":{"input":{"turn_detection":null}}}),
                false,
            )
            .await
        }
        "audio_truncate" => Probe::open(AUDIO, json!({}), false).await,
        "tool_call" => Probe::open(TEXT, json!({}), true).await,
        _ => Probe::open(TEXT, json!({}), false).await,
    };
    let mut probe = match probe {
        Ok(probe) => probe,
        Err(verdict) => return (Err(verdict), BTreeSet::new()),
    };
    let outcome = match name {
        "session" => session(&probe),
        "text_response" => text_response(&mut probe).await,
        "cancel" => cancel(&mut probe).await,
        "out_of_band" => out_of_band(&mut probe).await,
        "error_event_id" => error_event_id(&mut probe).await,
        "items" => items(&mut probe).await,
        "tool_call" => tool_call(&mut probe).await,
        "audio_input" => audio_input(&mut probe).await,
        _ => audio_truncate(&mut probe).await,
    };
    (outcome, probe.finish().await)
}

fn session(probe: &Probe) -> Outcome {
    let kinds: Vec<_> = probe
        .seen
        .iter()
        .filter_map(|e| e["type"].as_str())
        .collect();
    in_order(&kinds, &["session.created", "session.updated"])?;
    check(
        probe
            .seen
            .last()
            .is_some_and(|e| e["session"]["type"] == "realtime"),
        "session.updated does not report type realtime",
    )
}

/// A user item is added, then a text response follows the documented lifecycle.
async fn text_response(probe: &mut Probe) -> Outcome {
    let from = probe.seen.len();
    probe.send(text("Say hello in three words."))?;
    probe.item(|item| item["role"] == "user").await?;
    probe.send(respond(json!({"metadata":{"probe":"text"}})))?;
    let done = probe.until("response.done").await?;
    let response = &done["response"];
    let id = response["id"].as_str().unwrap_or("");
    in_order(
        &probe.kinds(from, id),
        &[
            "response.created",
            "response.output_item.added",
            "response.content_part.added",
            "response.output_text.delta",
            "response.output_text.done",
            "response.content_part.done",
            "response.output_item.done",
            "response.done",
        ],
    )?;
    check(
        response["status"] == "completed",
        "response did not complete",
    )?;
    check(
        response["metadata"]["probe"] == "text",
        "response.done does not echo request metadata",
    )
}

async fn cancel(probe: &mut Probe) -> Outcome {
    probe.send(text(
        "Count from one to two hundred in words, one per line.",
    ))?;
    probe.send(respond(json!({})))?;
    let created = probe.until("response.created").await?;
    let id = created["response"]["id"].as_str().unwrap_or("").to_owned();
    probe.send(Command::Interrupt {
        response_id: id,
        heard: vec![],
    })?;
    let done = probe.until("response.done").await?;
    match done["response"]["status"].as_str() {
        Some("cancelled") => Ok(()),
        Some("completed") => Err(Verdict::Skip("response completed before cancel".into())),
        other => fail(format!("cancelled response reported status {other:?}")),
    }
}

/// `conversation: none` runs outside the conversation, on its own input.
async fn out_of_band(probe: &mut Probe) -> Outcome {
    let from = probe.seen.len();
    probe.send(respond(json!({
        "conversation":"none",
        "metadata":{"probe":"oob"},
        "input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Reply with only the word banana."}]}],
    })))?;
    let created = probe.until("response.created").await?;
    check(
        created["response"].get("conversation_id") == Some(&Value::Null),
        "out-of-band response.created lacks conversation_id: null",
    )?;
    let done = probe.until("response.done").await?;
    let id = done["response"]["id"].as_str().unwrap_or("");
    check(
        !probe.seen[from..]
            .iter()
            .any(|e| ITEM_ACK.iter().any(|kind| e["type"] == *kind)),
        "out-of-band output was added to the conversation",
    )?;
    check(
        transcript(probe, from, id)
            .to_lowercase()
            .contains("banana"),
        "out-of-band response ignored its input",
    )?;
    check(
        done["response"]["metadata"]["probe"] == "oob",
        "out-of-band response.done does not echo metadata",
    )
}

async fn error_event_id(probe: &mut Probe) -> Outcome {
    probe.send(event(json!({
        "type":"conversation.item.delete",
        "event_id":"evt_conformance_missing",
        "item_id":"item_conformance_missing",
    })))?;
    let error = probe.until("error").await?;
    check(
        error["error"]["event_id"] == "evt_conformance_missing",
        &format!("error does not name the client event that caused it: {error}"),
    )
}

/// Client-chosen item IDs survive create, retrieve and delete.
async fn items(probe: &mut Probe) -> Outcome {
    let id = "item_conformance_1";
    probe.send(event(json!({"type":"conversation.item.create","item":{
        "id":id,"type":"message","role":"system",
        "content":[{"type":"input_text","text":"The secret word is lighthouse."}]}})))?;
    probe.item(|item| item["id"] == id).await?;
    probe.send(event(
        json!({"type":"conversation.item.retrieve","item_id":id}),
    ))?;
    let retrieved = probe.until("conversation.item.retrieved").await?;
    check(retrieved["item"]["id"] == id, "retrieved the wrong item")?;
    probe.send(event(
        json!({"type":"conversation.item.delete","item_id":id}),
    ))?;
    let deleted = probe.until("conversation.item.deleted").await?;
    check(deleted["item_id"] == id, "deleted the wrong item")
}

/// A required tool call runs once, its result is accepted, and the answer uses it.
async fn tool_call(probe: &mut Probe) -> Outcome {
    let from = probe.seen.len();
    probe.send(text(
        "Call the echo tool with text lighthouse, then tell me exactly what it returned.",
    ))?;
    probe.send(respond(json!({"tool_choice":"required"})))?;
    let first = probe.until("response.done").await?;
    let calls: Vec<_> = first["response"]["output"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|item| item["type"] == "function_call")
        .collect();
    check(
        calls.len() == 1,
        "tool_choice required did not produce one call",
    )?;
    check(calls[0]["name"] == "echo", "called an unknown tool")?;
    let first_id = first["response"]["id"].as_str().unwrap_or("").to_owned();
    in_order(
        &probe.kinds(from, &first_id),
        &[
            "response.function_call_arguments.done",
            "response.output_item.done",
            "response.done",
        ],
    )?;
    probe
        .item(|item| {
            item["type"] == "function_call_output" && item["call_id"] == calls[0]["call_id"]
        })
        .await?;
    let after = probe.seen.len();
    let done = probe.until("response.done").await?;
    let id = done["response"]["id"].as_str().unwrap_or("");
    check(
        transcript(probe, after, id)
            .to_lowercase()
            .contains("lighthouse"),
        "continuation did not use the tool result",
    )
}

/// Manual commit and clear, with turn detection disabled.
async fn audio_input(probe: &mut Probe) -> Outcome {
    let silence = STANDARD.encode(vec![0u8; 24_000 * 2 / 5]);
    probe.send(Command::Audio {
        audio: silence.clone(),
    })?;
    probe.send(Command::ClearAudio)?;
    probe.until("input_audio_buffer.cleared").await?;
    probe.send(Command::Audio { audio: silence })?;
    probe.send(Command::CommitAudio)?;
    let committed = probe.until("input_audio_buffer.committed").await?;
    probe
        .item(|item| item["id"] == committed["item_id"])
        .await?;
    Ok(())
}

/// Truncation after 300 ms of heard audio is confirmed at exactly that position.
async fn audio_truncate(probe: &mut Probe) -> Outcome {
    probe.send(text("Count slowly from one to thirty."))?;
    probe.send(respond(json!({})))?;
    let mut bytes = 0;
    let delta = loop {
        let event = probe.next().await?;
        if event["type"] == "response.done" {
            return Err(Verdict::Skip(
                "response ended before 500 ms of audio".into(),
            ));
        }
        if event["type"] == "response.output_audio.delta" {
            bytes += STANDARD
                .decode(event["delta"].as_str().unwrap_or(""))
                .map_or(0, |b| b.len());
            // 500 ms of 24 kHz PCM16; negotiated formats other than PCM are skipped below.
            if bytes >= 24_000 {
                break event;
            }
        }
    };
    let format = probe
        .seen
        .iter()
        .rev()
        .find(|e| e["type"] == "session.updated");
    if format.is_some_and(|e| {
        e["session"]["audio"]["output"]["format"]["type"]
            .as_str()
            .is_some_and(|t| t != "audio/pcm")
    }) {
        return Err(Verdict::Skip("output audio is not PCM16".into()));
    }
    let item = delta["item_id"].as_str().unwrap_or("").to_owned();
    probe.send(Command::Interrupt {
        response_id: delta["response_id"].as_str().unwrap_or("").into(),
        heard: vec![Heard {
            item_id: item.clone(),
            content_index: 0,
            audio_end_ms: 300,
        }],
    })?;
    let truncated = probe.until("conversation.item.truncated").await?;
    check(
        truncated["item_id"] == item.as_str() && truncated["audio_end_ms"] == 300,
        "truncation was not confirmed at the heard position",
    )
}

#[tokio::main]
async fn main() {
    if std::env::args().any(|arg| arg == "--help") {
        println!(
            "Usage: cargo run --locked --example conformance [CHECK...]\nSet REALTIME_URL; optional REALTIME_MODEL and REALTIME_TOKEN.\nChecks: {}",
            CHECKS.join(", ")
        );
        return;
    }
    let requested: Vec<String> = std::env::args().skip(1).collect();
    let mut failed = false;
    let mut unknown = BTreeSet::new();
    for name in CHECKS
        .iter()
        .filter(|c| requested.is_empty() || requested.iter().any(|r| r == *c))
    {
        let (outcome, seen) = run(name).await;
        unknown.extend(seen);
        let (result, detail) = match outcome {
            Ok(()) => ("pass", None),
            Err(Verdict::Skip(why)) => ("skip", Some(why)),
            Err(Verdict::Fail(why)) => {
                failed = true;
                ("fail", Some(why))
            }
        };
        println!("{}", json!({"check":name,"result":result,"detail":detail}));
    }
    println!(
        "{}",
        json!({"conformant":!failed,"nonstandard_events":unknown})
    );
    if failed {
        std::process::exit(1);
    }
}

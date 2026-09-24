use crate::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    panic::AssertUnwindSafe,
};
use tokio::{
    task::JoinSet,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    Connector, connect_async_tls_with_config,
    tungstenite::{Message, client::IntoClientRequest, protocol::WebSocketConfig},
};
use uuid::Uuid;

fn id() -> String {
    format!("rh_{}", Uuid::new_v4().simple())
}
fn field<'a>(value: &'a Value, name: &str) -> Result<&'a str, Error> {
    value[name]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Protocol(format!("missing {name}")))
}

pub(crate) async fn connect(
    mut config: Config,
    registry: Arc<ToolRegistry>,
) -> Result<Session, Error> {
    let l = &config.limits;
    if [
        l.queue,
        l.message_bytes,
        l.audio_chunk_bytes,
        l.tool_argument_bytes,
        l.tool_result_bytes,
        l.concurrent_tools,
        l.calls,
        l.responses,
        l.audio_parts,
    ]
    .contains(&0)
    {
        return Err(Error::Config("limits must be positive".into()));
    }
    if [
        l.connect_timeout,
        l.initialize_timeout,
        l.write_timeout,
        l.idle_timeout,
        l.tool_timeout,
        l.acknowledgement_timeout,
    ]
    .iter()
    .any(Duration::is_zero)
    {
        return Err(Error::Config("timeouts must be positive".into()));
    }
    if matches!(
        config.capabilities.output_audio,
        AudioFormat::Pcm16 { sample_rate: 0 }
    ) {
        return Err(Error::Config("sample rate must be positive".into()));
    }
    let mut url =
        url::Url::parse(&config.endpoint).map_err(|_| Error::Config("endpoint URL".into()))?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !(url.scheme() == "wss" || url.scheme() == "ws" && loopback)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Config(
            "use wss or loopback ws, without URL credentials or fragment".into(),
        ));
    }
    if let Some(model) = &config.model {
        let pairs: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "model")
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        url.query_pairs_mut()
            .clear()
            .extend_pairs(pairs)
            .append_pair("model", model);
    }
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| Error::Config("endpoint request".into()))?;
    if let Some(token) = config.bearer_token.take() {
        let mut header = format!("Bearer {token}")
            .parse::<tokio_tungstenite::tungstenite::http::HeaderValue>()
            .map_err(|_| Error::Config("invalid bearer token".into()))?;
        header.set_sensitive(true);
        request.headers_mut().insert("authorization", header);
    }
    if !config.session.is_object() || config.session.get("tools").is_some() {
        return Err(Error::Config(
            "session must be an object without tools; use ToolRegistry".into(),
        ));
    }
    config.session["tools"] = Value::Array(registry.definitions.clone());
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(l.message_bytes))
        .max_frame_size(Some(l.message_bytes));
    // Choose per connection: another crate may enable a second crypto provider
    // or native TLS. Do not depend on or replace the host's global TLS defaults.
    let connector = if url.scheme() == "wss" {
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| Error::Config("TLS protocol versions".into()))?
        .with_root_certificates(rustls::RootCertStore::from_iter(
            webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
        ))
        .with_no_client_auth();
        Some(Connector::Rustls(Arc::new(tls)))
    } else {
        None
    };
    let (socket, _) = timeout(
        l.connect_timeout,
        connect_async_tls_with_config(request, Some(ws_config), true, connector),
    )
    .await
    .map_err(|_| Error::Timeout("connect"))?
    .map_err(|error| match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            Error::Connect(format!("HTTP {}", response.status().as_u16()))
        }
        _ => Error::Connect("WebSocket handshake or transport".into()),
    })?;
    let (tx, commands) = mpsc::channel(l.queue);
    let (control, controls) = mpsc::channel(8);
    let (events_tx, events) = mpsc::channel(l.queue);
    let (status_tx, status) = watch::channel(Status::Connecting);
    let (playback_tx, playback) = watch::channel(PlaybackState::default());
    let shutdown = CancellationToken::new();
    let handle = Handle {
        tx,
        control,
        max_bytes: l.message_bytes,
        max_audio_bytes: l.audio_chunk_bytes,
    };
    let cancel = shutdown.clone();
    let task = tokio::spawn(async move {
        let result = run(
            socket,
            config,
            registry,
            commands,
            controls,
            events_tx,
            status_tx.clone(),
            playback_tx.clone(),
            cancel,
        )
        .await;
        playback_tx.send_modify(|state| state.terminal = true);
        status_tx.send_replace(match &result {
            Ok(()) => Status::Closed,
            Err(e) => Status::Failed(e.clone()),
        });
        result
    });
    Ok(Session {
        handle,
        events,
        status,
        playback,
        shutdown,
        task: Some(task),
    })
}

#[derive(Default)]
struct Response {
    created: bool,
    done: bool,
    successful: bool,
    cancelled: bool,
    calls: HashSet<String>,
    continued: bool,
}
struct Call {
    response: String,
    name: String,
    arguments: String,
    cancel: CancellationToken,
    result_item: Option<String>,
    accepted: bool,
    finished: bool,
    deadline: Option<Instant>,
}
#[derive(Default)]
struct AudioPart {
    response: String,
    bytes: u64,
    format: AudioFormat,
    heard: u64,
    truncated: bool,
}
struct ToolResult {
    call_id: String,
    value: Value,
}

enum Write {
    Frame(Message),
    Flush,
}

struct Coordinator {
    config: Config,
    registry: Arc<ToolRegistry>,
    outgoing: mpsc::Sender<Write>,
    urgent: mpsc::Sender<Write>,
    events: mpsc::Sender<Event>,
    playback: watch::Sender<PlaybackState>,
    responses: HashMap<String, Response>,
    calls: HashMap<String, Call>,
    audio: HashMap<(String, u32), AudioPart>,
    active: HashSet<String>,
    tasks: JoinSet<ToolResult>,
    ready: bool,
    frankie_feedback: bool,
    init_id: String,
    create_id: Option<(String, Instant)>,
}

#[allow(clippy::too_many_arguments)]
async fn run(
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    config: Config,
    registry: Arc<ToolRegistry>,
    mut commands: mpsc::Receiver<Command>,
    mut controls: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
    status: watch::Sender<Status>,
    playback: watch::Sender<PlaybackState>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let (mut sink, mut stream) = socket.split();
    let (outgoing, mut writes) = mpsc::channel::<Write>(config.limits.queue);
    let (urgent, mut priority_writes) = mpsc::channel::<Write>(16);
    let write_timeout = config.limits.write_timeout;
    let mut writer = tokio::spawn(async move {
        loop {
            let write = tokio::select! {
                biased;
                write = priority_writes.recv() => write,
                write = writes.recv() => write,
            };
            let Some(write) = write else { break };
            timeout(write_timeout, async {
                match write {
                    Write::Frame(message) => sink.send(message).await,
                    Write::Flush => sink.flush().await,
                }
            })
            .await
            .map_err(|_| Error::Timeout("write"))?
            .map_err(|_| Error::Disconnected)?;
        }
        Ok::<_, Error>(())
    });
    let init_deadline = Instant::now() + config.limits.initialize_timeout;
    let mut c = Coordinator {
        config,
        registry,
        outgoing,
        urgent,
        events,
        playback,
        responses: HashMap::new(),
        calls: HashMap::new(),
        audio: HashMap::new(),
        active: HashSet::new(),
        tasks: JoinSet::new(),
        ready: false,
        frankie_feedback: false,
        init_id: id(),
        create_id: None,
    };
    let mut writer_joined = false;
    let mut result = async {
        c.send(json!({"type":"session.update","event_id":c.init_id,"session":c.config.session}))?;
        let mut tick = tokio::time::interval(Duration::from_millis(25));
        let mut received = Instant::now();
        let mut ping_sent = false;
        loop {
            if shutdown.is_cancelled() {return Ok(());}
            if let Ok(command) = controls.try_recv() {
                if matches!(command,Command::Close) { return Ok(()); }
                c.accept_command(command)?;
                continue;
            }
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                result = &mut writer => { writer_joined = true; return result.map_err(|_| Error::Disconnected)?.and(Err(Error::Disconnected)); },
                _ = c.events.closed() => return Ok(()),
                command = controls.recv() => match command {
                    Some(Command::Close) | None => return Ok(()),
                    Some(command) => c.accept_command(command)?,
                },
                _ = tick.tick() => {
                    let now = Instant::now();
                    if now.duration_since(received) >= c.config.limits.idle_timeout { return Err(Error::Timeout("peer liveness")); }
                    if !ping_sent && now.duration_since(received) >= c.config.limits.idle_timeout / 2 {
                        c.enqueue(Write::Frame(Message::Ping(Vec::new().into())),true)?;
                        ping_sent = true;
                    }
                    if !c.ready && now >= init_deadline { return Err(Error::Timeout("session initialization")); }
                    if c.calls.values().any(|call| call.deadline.is_some_and(|t| now >= t)) {
                        return Err(Error::Timeout("tool result acknowledgement"));
                    }
                    if c.create_id.as_ref().is_some_and(|(_,t)| now >= *t) {
                        return Err(Error::Timeout("response creation"));
                    }
                }
                command = commands.recv(), if c.ready => match command {
                    Some(Command::Close) | None => return Ok(()),
                    Some(command) => c.accept_command(command)?,
                },
                result = c.tasks.join_next(), if !c.tasks.is_empty() => {
                    let result = result.expect("nonempty tasks").map_err(|_| Error::Protocol("tool worker failed".into()))?;
                    c.tool_result(result)?;
                }
                message = stream.next() => {
                    received=Instant::now(); ping_sent=false;
                    match message {
                    Some(Ok(Message::Text(text))) => {
                        let event: Value = serde_json::from_str(&text).map_err(|_| Error::Protocol("invalid server JSON".into()))?;
                        c.server(event)?;
                        if c.ready && *status.borrow() != Status::Ready { status.send_replace(Status::Ready); }
                    }
                    Some(Ok(Message::Ping(_))) => c.enqueue(Write::Flush,true)?,
                    Some(Ok(Message::Pong(_))) => {},
                    Some(Ok(Message::Close(_))) | None => return Err(Error::Disconnected),
                    Some(Err(_)) => return Err(Error::Disconnected),
                    _ => return Err(Error::Protocol("expected JSON text frame".into())),
                    }
                }
            }
        }
    }.await;
    for call in c.calls.values() {
        call.cancel.cancel();
    }
    c.tasks.abort_all();
    while c.tasks.join_next().await.is_some() {}
    // No queued write is replayed after shutdown. A write already sent may have taken effect.
    if !writer_joined {
        if writer.is_finished() && matches!(result, Err(Error::Disconnected)) {
            if let Ok(Err(error)) = writer.await {
                result = Err(error);
            }
        } else {
            writer.abort();
            let _ = writer.await;
        }
    }
    result
}

impl Coordinator {
    fn enqueue(&self, write: Write, urgent: bool) -> Result<(), Error> {
        let tx = if urgent { &self.urgent } else { &self.outgoing };
        tx.try_send(write).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Capacity("socket write queue"),
            mpsc::error::TrySendError::Closed(_) => Error::Disconnected,
        })
    }
    fn send(&self, mut value: Value) -> Result<(), Error> {
        if value.get("event_id").is_none() {
            value["event_id"] = json!(id());
        }
        let text =
            serde_json::to_string(&value).map_err(|_| Error::Protocol("event encoding".into()))?;
        if text.len() > self.config.limits.message_bytes {
            return Err(Error::Capacity("outgoing message"));
        }
        let urgent = matches!(
            value["type"].as_str(),
            Some("response.cancel" | "conversation.item.truncate")
        );
        self.enqueue(Write::Frame(Message::Text(text.into())), urgent)
    }
    fn emit(&self, event: Event) -> Result<(), Error> {
        self.events.try_send(event).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Capacity("consumer event queue"),
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })
    }
    fn response(&mut self, rid: &str) -> Result<&mut Response, Error> {
        if !self.responses.contains_key(rid) && self.responses.len() >= self.config.limits.responses
        {
            return Err(Error::Capacity("response ledger"));
        }
        Ok(self.responses.entry(rid.to_owned()).or_default())
    }
    fn create_response(&mut self) -> Result<(), Error> {
        if !self.active.is_empty() || self.create_id.is_some() {
            return Err(Error::Protocol(
                "response already active or requested".into(),
            ));
        }
        let event_id = id();
        self.send(json!({"type":"response.create","event_id":event_id}))?;
        self.create_id = Some((
            event_id,
            Instant::now() + self.config.limits.acknowledgement_timeout,
        ));
        Ok(())
    }
    fn accept_command(&mut self, command: Command) -> Result<(), Error> {
        if let Err(error) = self.command(command) {
            match error {
                Error::Config(_) | Error::Protocol(_) => self.emit(Event::CommandRejected {
                    error: error.to_string(),
                })?,
                other => return Err(other),
            }
        }
        Ok(())
    }
    fn command(&mut self, command: Command) -> Result<(), Error> {
        match command {
            Command::Frankie { event } => self.frankie(event)?,
            Command::Text { text } => {
                self.user_item(vec![json!({"type":"input_text","text":text})])?
            }
            Command::Image { image_url, text } => {
                if !self.config.capabilities.image_input {
                    return Err(Error::Config("image input disabled".into()));
                }
                let mut content = vec![json!({"type":"input_image","image_url":image_url})];
                if let Some(text) = text {
                    content.push(json!({"type":"input_text","text":text}));
                }
                self.user_item(content)?;
            }
            Command::Audio { audio } => {
                if !self.config.capabilities.audio_input {
                    return Err(Error::Config("audio input disabled".into()));
                }
                STANDARD
                    .decode(&audio)
                    .map_err(|_| Error::Protocol("invalid input audio base64".into()))?;
                self.send(json!({"type":"input_audio_buffer.append","audio":audio}))?;
            }
            Command::CommitAudio => self.send(json!({"type":"input_audio_buffer.commit"}))?,
            Command::ClearAudio => self.send(json!({"type":"input_audio_buffer.clear"}))?,
            Command::Respond => self.create_response()?,
            Command::Interrupt { response_id, heard } => {
                // Validate every position before issuing any cancellation or truncation.
                self.validate_heard(&response_id, &heard)?;
                if self.active.contains(&response_id) {
                    self.send(json!({"type":"response.cancel","response_id":response_id}))?;
                }
                self.clear_playback(&response_id)?;
                self.truncate(&response_id, &heard)?;
            }
            Command::PlaybackStopped { response_id, heard } => {
                self.validate_heard(&response_id, &heard)?;
                self.clear_playback(&response_id)?;
                self.truncate(&response_id, &heard)?;
            }
            Command::CancelTool { call_id } => {
                let call = self
                    .calls
                    .get(&call_id)
                    .ok_or_else(|| Error::Protocol("unknown tool call".into()))?;
                if !call.finished {
                    call.cancel.cancel();
                }
            }
            Command::Close => unreachable!(),
        }
        Ok(())
    }
    fn frankie(&mut self, event: Value) -> Result<(), Error> {
        if !self.config.frankie_extensions || !self.frankie_feedback {
            return Err(Error::Config(
                "Frankie extensions disabled or not advertised".into(),
            ));
        }
        match event["type"].as_str() {
            Some("input_audio_buffer.append") => {
                if !self.config.capabilities.audio_input {
                    return Err(Error::Config("audio input disabled".into()));
                }
                let audio = STANDARD
                    .decode(field(&event, "audio")?)
                    .map_err(|_| Error::Protocol("invalid input audio base64".into()))?;
                let playback = STANDARD
                    .decode(field(&event, "playback")?)
                    .map_err(|_| Error::Protocol("invalid playback audio base64".into()))?;
                if audio.len() != playback.len() {
                    return Err(Error::Protocol(
                        "microphone and playback frames must align".into(),
                    ));
                }
            }
            Some("frankie.playback.position" | "frankie.playback.finished") => {
                let item = field(&event, "item_id")?;
                let rid = field(&event, "response_id")?;
                let end = event["audio_end_ms"]
                    .as_u64()
                    .ok_or_else(|| Error::Protocol("invalid playback position".into()))?;
                let h = Heard {
                    item_id: item.to_owned(),
                    content_index: 0,
                    audio_end_ms: end,
                };
                self.validate_heard(rid, std::slice::from_ref(&h))?;
                self.audio
                    .get_mut(&(h.item_id, 0))
                    .expect("validated")
                    .heard = end;
            }
            _ => return Err(Error::Config("unsupported Frankie extension".into())),
        }
        self.send(event)
    }
    fn user_item(&self, content: Vec<Value>) -> Result<(), Error> {
        self.send(json!({"type":"conversation.item.create","item":{"id":id(),"type":"message","role":"user","content":content}}))
    }
    fn validate_heard(&self, rid: &str, heard: &[Heard]) -> Result<(), Error> {
        if !self.config.capabilities.truncate && self.audio.values().any(|p| p.response == rid) {
            return Err(Error::Config(
                "endpoint truncation disabled for audio".into(),
            ));
        }
        if !self.responses.contains_key(rid) {
            return Err(Error::Protocol("unknown playback response".into()));
        }
        let mut seen = HashSet::new();
        for h in heard {
            let key = (h.item_id.clone(), h.content_index);
            let part = self
                .audio
                .get(&key)
                .ok_or_else(|| Error::Protocol("unknown audio part".into()))?;
            if part.response != rid
                || !seen.insert(key)
                || h.audio_end_ms < part.heard
                || h.audio_end_ms > part.format.duration_ms(part.bytes)
                || part.truncated && h.audio_end_ms != part.heard
            {
                return Err(Error::Protocol(
                    "invalid or regressing heard position".into(),
                ));
            }
        }
        Ok(())
    }
    fn clear_playback(&mut self, rid: &str) -> Result<(), Error> {
        self.response(rid)?.cancelled = true;
        let fresh = !self.playback.borrow().stopped.contains(rid);
        self.playback.send_modify(|state| {
            state.stopped.insert(rid.to_owned());
        });
        if fresh {
            self.emit(Event::PlaybackClear {
                response_id: rid.to_owned(),
            })?;
        }
        Ok(())
    }
    fn truncate(&mut self, rid: &str, heard: &[Heard]) -> Result<(), Error> {
        for h in heard {
            self.audio
                .get_mut(&(h.item_id.clone(), h.content_index))
                .expect("validated")
                .heard = h.audio_end_ms;
        }
        let parts: Vec<_> = self
            .audio
            .iter()
            .filter(|(_, p)| p.response == rid && !p.truncated)
            .map(|((item, index), p)| (item.clone(), *index, p.heard))
            .collect();
        for (item, index, end) in parts {
            self.send(json!({"type":"conversation.item.truncate","item_id":item,"content_index":index,"audio_end_ms":end}))?;
            self.audio.get_mut(&(item, index)).expect("known").truncated = true;
        }
        Ok(())
    }
    fn server(&mut self, event: Value) -> Result<(), Error> {
        let kind = field(&event, "type")?;
        let rid = event["response_id"].as_str();
        let public_media = kind.starts_with("response.output_audio.")
            || kind.starts_with("response.output_audio_transcript.")
            || kind.starts_with("response.output_text.")
            || kind == "response.content_part.done"
            || kind == "response.output_item.done" && event["item"]["type"] == "message";
        if public_media
            && rid.is_some_and(|id| {
                self.responses
                    .get(id)
                    .is_some_and(|r| r.cancelled || r.done)
            })
        {
            return Ok(());
        }
        match kind {
            "session.updated" => {
                self.frankie_feedback = event
                    .pointer("/session/frankie/playback_feedback")
                    .and_then(Value::as_bool)
                    == Some(true);
                if let Some(format) = event.pointer("/session/audio/output/format") {
                    self.config.capabilities.output_audio = match format["type"].as_str() {
                        Some("audio/pcm") => {
                            let sample_rate = format
                                .get("rate")
                                .map_or(Some(24_000), Value::as_u64)
                                .and_then(|n| u32::try_from(n).ok())
                                .filter(|n| *n > 0)
                                .ok_or_else(|| {
                                    Error::Protocol("invalid negotiated PCM rate".into())
                                })?;
                            AudioFormat::Pcm16 { sample_rate }
                        }
                        Some("audio/pcmu" | "audio/pcma") => AudioFormat::G711,
                        _ => {
                            return Err(Error::Protocol(
                                "unsupported negotiated output audio format".into(),
                            ));
                        }
                    };
                }
                self.ready = true;
            }
            "response.created" => {
                let rid = field(&event["response"], "id")?;
                let state = self.response(rid)?;
                if !state.created {
                    state.created = true;
                    if !state.done {
                        self.active.insert(rid.to_owned());
                    }
                    self.create_id = None;
                }
            }
            "response.output_audio.delta" => {
                let rid = field(&event, "response_id")?;
                self.response(rid)?;
                let item = field(&event, "item_id")?;
                let index = event["content_index"]
                    .as_u64()
                    .and_then(|i| u32::try_from(i).ok())
                    .ok_or_else(|| Error::Protocol("invalid audio content index".into()))?;
                let delta = field(&event, "delta")?;
                if delta.len() > self.config.limits.audio_chunk_bytes {
                    return Err(Error::Capacity("output audio chunk"));
                }
                let bytes = STANDARD
                    .decode(delta)
                    .map_err(|_| Error::Protocol("invalid output audio base64".into()))?;
                let key = (item.to_owned(), index);
                if !self.audio.contains_key(&key)
                    && self.audio.len() >= self.config.limits.audio_parts
                {
                    return Err(Error::Capacity("audio ledger"));
                }
                let part = self.audio.entry(key).or_insert_with(|| AudioPart {
                    response: rid.to_owned(),
                    format: self.config.capabilities.output_audio,
                    ..Default::default()
                });
                if part.response != rid || part.format != self.config.capabilities.output_audio {
                    return Err(Error::Protocol(
                        "audio item response or format conflict".into(),
                    ));
                }
                part.bytes = part
                    .bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or(Error::Capacity("audio duration"))?;
            }
            "response.output_item.done" => {
                let rid = field(&event, "response_id")?;
                if event["item"]["type"] == "function_call"
                    && event["item"]["status"] == "completed"
                {
                    self.admit(rid, &event["item"])?;
                }
            }
            "response.done" => {
                let response = &event["response"];
                let rid = field(response, "id")?;
                let completed = response["status"] == "completed";
                let was_cancelled = self.response(rid)?.cancelled;
                if completed
                    && !was_cancelled
                    && let Some(output) = response["output"].as_array()
                {
                    for item in output {
                        if item["type"] == "function_call" && item["status"] == "completed" {
                            self.admit(rid, item)?;
                        }
                    }
                }
                let state = self.response(rid)?;
                state.done = true;
                state.successful = completed;
                self.active.remove(rid);
                if response["status"] == "cancelled" {
                    self.clear_playback(rid)?;
                }
                self.continue_ready()?;
            }
            "conversation.item.added" | "conversation.item.done" | "conversation.item.created" => {
                let item = &event["item"];
                if item["type"] == "function_call_output" {
                    if let Some(call) = item["call_id"]
                        .as_str()
                        .and_then(|id| self.calls.get_mut(id))
                        && call.result_item.as_deref() == item["id"].as_str()
                        && call.result_item.is_some()
                    {
                        call.accepted = true;
                        call.deadline = None;
                    }
                    self.continue_ready()?;
                }
            }
            "error" => {
                let event_id = event["error"]["event_id"].as_str();
                if !self.ready || event_id == Some(self.init_id.as_str()) {
                    self.emit(Event::Server { event })?;
                    return Err(Error::Initialization);
                }
                if self.calls.values().any(|call| {
                    call.result_item.as_deref() == event_id && call.result_item.is_some()
                }) {
                    self.emit(Event::Server { event })?;
                    return Err(Error::Protocol(
                        "tool result rejected; delivery not retried".into(),
                    ));
                }
                if self
                    .create_id
                    .as_ref()
                    .is_some_and(|(id, _)| Some(id.as_str()) == event_id)
                {
                    self.create_id = None;
                    // No retry: the consumer sees the error and chooses the next action.
                }
            }
            _ => {}
        }
        self.emit(Event::Server { event })
    }
    fn admit(&mut self, rid: &str, item: &Value) -> Result<(), Error> {
        if self.response(rid)?.cancelled {
            return Ok(());
        }
        let call_id = field(item, "call_id")?.to_owned();
        let name = field(item, "name")?.to_owned();
        let raw = field(item, "arguments")?;
        if raw.len() > self.config.limits.tool_argument_bytes {
            return Err(Error::Capacity("tool arguments"));
        }
        let parsed = serde_json::from_str::<Value>(raw).map(|mut value| {
            // Cargo features are additive across the embedding application.
            value.sort_all_objects();
            value
        });
        let fingerprint = parsed
            .as_ref()
            .map(|v| v.to_string())
            .unwrap_or_else(|_| raw.to_owned());
        if let Some(existing) = self.calls.get(&call_id) {
            if existing.response != rid
                || existing.name != name
                || existing.arguments != fingerprint
            {
                return Err(Error::Protocol("conflicting duplicate tool call ID".into()));
            }
            return Ok(());
        }
        if self.response(rid)?.done {
            return Err(Error::Protocol("new tool call after response ended".into()));
        }
        if self.calls.len() >= self.config.limits.calls {
            return Err(Error::Capacity("tool call ledger"));
        }
        if self.tasks.len() >= self.config.limits.concurrent_tools {
            return Err(Error::Capacity("concurrent tools"));
        }
        let cancel = CancellationToken::new();
        self.calls.insert(
            call_id.clone(),
            Call {
                response: rid.to_owned(),
                name: name.clone(),
                arguments: fingerprint,
                cancel: cancel.clone(),
                result_item: None,
                accepted: false,
                finished: false,
                deadline: None,
            },
        );
        self.response(rid)?.calls.insert(call_id.clone());
        self.emit(Event::Tool {
            call_id: call_id.clone(),
            state: "admitted",
        })?;
        let registry = self.registry.clone();
        let duration = self.config.limits.tool_timeout;
        self.tasks.spawn(async move {
            let work=async {
                let arguments=parsed.map_err(|_|"invalid_arguments_json".to_owned())?;
                let validator=registry.validators.get(&name).ok_or_else(||"unknown_tool".to_owned())?;
                if !validator.is_valid(&arguments) { return Err("invalid_arguments_schema".into()); }
                registry.executor.execute(ToolCall{call_id:call_id.clone(),name,arguments},cancel.clone()).await
            };
            let result=tokio::select! {
                biased;
                _=cancel.cancelled()=>Err("cancelled; external side effects may already have occurred".to_owned()),
                result=timeout(duration,AssertUnwindSafe(work).catch_unwind())=>match result {
                    Ok(Ok(value))=>value,
                    Ok(Err(_))=>Err("tool_panicked".into()),
                    Err(_)=>{ cancel.cancel(); Err("tool_timeout; external side effects may already have occurred".into()) },
                },
            };
            ToolResult{call_id,value:match result {Ok(v)=>v,Err(e)=>json!({"error":e})}}
        });
        Ok(())
    }
    fn tool_result(&mut self, result: ToolResult) -> Result<(), Error> {
        let mut output = result.value.to_string();
        if output.len() > self.config.limits.tool_result_bytes {
            output = json!({"error":"tool_result_too_large"}).to_string();
        }
        let item_id = id();
        self.send(json!({"type":"conversation.item.create","event_id":item_id,
            "item":{"id":item_id,"type":"function_call_output","call_id":result.call_id,"output":output}}))?;
        let call = self.calls.get_mut(&result.call_id).expect("admitted");
        call.finished = true;
        call.result_item = Some(item_id);
        call.deadline = Some(Instant::now() + self.config.limits.acknowledgement_timeout);
        self.emit(Event::Tool {
            call_id: result.call_id,
            state: "result_sent",
        })?;
        Ok(())
    }
    fn continue_ready(&mut self) -> Result<(), Error> {
        if self.config.continuation == Continuation::Server
            || !self.active.is_empty()
            || self.create_id.is_some()
        {
            return Ok(());
        }
        let ready: Vec<_> = self
            .responses
            .iter()
            .filter(|(_, r)| {
                r.done
                    && r.successful
                    && !r.cancelled
                    && !r.continued
                    && !r.calls.is_empty()
                    && r.calls.iter().all(|id| self.calls[id].accepted)
            })
            .map(|(id, _)| id.clone())
            .collect();
        if !ready.is_empty() {
            self.create_response()?;
            for id in ready {
                self.responses.get_mut(&id).expect("known").continued = true;
            }
        }
        Ok(())
    }
}

use crate::*;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::json;
use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher, RandomState},
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
    /// The reported `conversation_id`: `Some(false)` when null (out-of-band: never
    /// gates the default conversation, calls never execute), `Some(true)` when set.
    conversation: Option<bool>,
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
    init_deadline: Instant,
    create_id: Option<(String, Instant)>,
    received: Instant,
    ping_sent: bool,
    oversized_key: RandomState,
    /// Once out-of-band responses exist, only calls from a response that reports its
    /// conversation may execute.
    out_of_band_requested: bool,
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Owns the socket for one session. Every `select!` arm delegates to one Coordinator
/// method so the protocol state machine is readable and formatted outside the macro.
#[allow(clippy::too_many_arguments)]
async fn run(
    socket: Socket,
    config: Config,
    registry: Arc<ToolRegistry>,
    mut commands: mpsc::Receiver<Command>,
    mut controls: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
    status: watch::Sender<Status>,
    playback: watch::Sender<PlaybackState>,
    shutdown: CancellationToken,
) -> Result<(), Error> {
    let (sink, mut stream) = socket.split();
    let (outgoing, writes) = mpsc::channel::<Write>(config.limits.queue);
    let (urgent, priority_writes) = mpsc::channel::<Write>(16);
    let mut writer = tokio::spawn(write_frames(
        sink,
        priority_writes,
        writes,
        config.limits.write_timeout,
    ));
    let now = Instant::now();
    let mut c = Coordinator {
        init_deadline: now + config.limits.initialize_timeout,
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
        received: now,
        ping_sent: false,
        oversized_key: RandomState::new(),
        out_of_band_requested: false,
    };
    let mut writer_joined = false;
    let mut result = async {
        c.send(json!({"type":"session.update","event_id":c.init_id,"session":c.config.session}))?;
        let mut tick = tokio::time::interval(Duration::from_millis(25));
        loop {
            if shutdown.is_cancelled() {
                return Ok(());
            }
            // Controls overtake ordinary commands even when both are ready.
            if let Ok(command) = controls.try_recv() {
                if !c.accept(Some(command))? {
                    return Ok(());
                }
                continue;
            }
            tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                _ = c.events.closed() => return Ok(()),
                joined = &mut writer => {
                    writer_joined = true;
                    return joined.map_err(|_| Error::Disconnected)?.and(Err(Error::Disconnected));
                }
                command = controls.recv() => if !c.accept(command)? { return Ok(()) },
                command = commands.recv(), if c.ready => if !c.accept(command)? { return Ok(()) },
                _ = tick.tick() => c.check_deadlines()?,
                joined = c.tasks.join_next(), if !c.tasks.is_empty() => c.tool_joined(joined)?,
                frame = stream.next() => {
                    c.frame(frame)?;
                    if c.ready && *status.borrow() != Status::Ready {
                        status.send_replace(Status::Ready);
                    }
                }
            }
        }
    }
    .await;
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

/// Urgent frames (cancel, truncate, pong flush) overtake queued media, never a frame in flight.
async fn write_frames(
    mut sink: futures_util::stream::SplitSink<Socket, Message>,
    mut priority: mpsc::Receiver<Write>,
    mut ordinary: mpsc::Receiver<Write>,
    write_timeout: Duration,
) -> Result<(), Error> {
    loop {
        let write = tokio::select! {
            biased;
            write = priority.recv() => write,
            write = ordinary.recv() => write,
        };
        let Some(write) = write else {
            return Ok(());
        };
        let io = async {
            match write {
                Write::Frame(message) => sink.send(message).await,
                Write::Flush => sink.flush().await,
            }
        };
        timeout(write_timeout, io)
            .await
            .map_err(|_| Error::Timeout("write"))?
            .map_err(|_| Error::Disconnected)?;
    }
}

/// Runs one validated call. Cancellation and timeout report that side effects may have happened.
async fn execute(
    registry: Arc<ToolRegistry>,
    call: ToolCall,
    cancel: CancellationToken,
    limit: Duration,
) -> ToolResult {
    let call_id = call.call_id.clone();
    let token = cancel.clone();
    // The executor is invoked on first poll, inside the guard: a panic while building
    // its future is a per-call result, like a panic while polling it.
    let work = AssertUnwindSafe(async move { registry.executor.execute(call, token).await })
        .catch_unwind();
    let result = tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            Err("cancelled; external side effects may already have occurred".to_owned())
        }
        finished = timeout(limit, work) => match finished {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("tool_panicked".into()),
            Err(_) => {
                cancel.cancel();
                Err("tool_timeout; external side effects may already have occurred".into())
            }
        },
    };
    ToolResult {
        call_id,
        value: result.unwrap_or_else(|error| json!({ "error": error })),
    }
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
    fn create_response(&mut self, parameters: Option<Value>) -> Result<(), Error> {
        let mut event = json!({"type":"response.create","event_id":id()});
        let out_of_band = parameters
            .as_ref()
            .is_some_and(|p| p["conversation"] == "none");
        if let Some(parameters) = parameters {
            if !parameters.is_object() {
                return Err(Error::Config(
                    "response parameters must be an object".into(),
                ));
            }
            if !out_of_band && parameters.get("tools").is_some() {
                return Err(Error::Config(
                    "default-conversation responses use the tool registry".into(),
                ));
            }
            event["response"] = parameters;
        }
        if out_of_band {
            // Parallel by design; the host correlates it through its own metadata.
            self.out_of_band_requested = true;
            return self.send(event);
        }
        if !self.active.is_empty() || self.create_id.is_some() {
            return Err(Error::Protocol(
                "response already active or requested".into(),
            ));
        }
        let event_id = event["event_id"].as_str().expect("set above").to_owned();
        self.send(event)?;
        self.create_id = Some((
            event_id,
            Instant::now() + self.config.limits.acknowledgement_timeout,
        ));
        Ok(())
    }
    /// Forwards a client event whose state the harness does not own.
    fn forward(&mut self, event: Value) -> Result<(), Error> {
        let kind = field(&event, "type")?;
        let owner = match kind {
            "response.create" => Some("the respond command"),
            "response.cancel" | "conversation.item.truncate" => {
                Some("the interrupt and playback_stopped commands")
            }
            "conversation.item.create" if event["item"]["type"] == "function_call_output" => {
                Some("the tool registry")
            }
            "session.update" if event["session"].get("tools").is_some() => {
                Some("the tool registry")
            }
            _ if kind.starts_with("frankie.") => Some("the frankie command"),
            _ => None,
        };
        if let Some(owner) = owner {
            return Err(Error::Config(format!("{kind} is owned by {owner}")));
        }
        self.send(event)
    }
    /// Returns false when the host closed the session. A caller mistake rejects only
    /// that command; resource and transport failures end the session.
    fn accept(&mut self, command: Option<Command>) -> Result<bool, Error> {
        let Some(command) = command.filter(|c| !matches!(c, Command::Close)) else {
            return Ok(false);
        };
        match self.command(command) {
            Err(error @ (Error::Config(_) | Error::Protocol(_))) => {
                self.emit(Event::CommandRejected {
                    error: error.to_string(),
                })?;
            }
            other => other?,
        }
        Ok(true)
    }
    fn check_deadlines(&mut self) -> Result<(), Error> {
        let now = Instant::now();
        let silent = now.duration_since(self.received);
        let idle = self.config.limits.idle_timeout;
        if silent >= idle {
            return Err(Error::Timeout("peer liveness"));
        }
        if !self.ping_sent && silent >= idle / 2 {
            self.enqueue(Write::Frame(Message::Ping(Vec::new().into())), true)?;
            self.ping_sent = true;
        }
        if !self.ready && now >= self.init_deadline {
            return Err(Error::Timeout("session initialization"));
        }
        if self
            .calls
            .values()
            .any(|call| call.deadline.is_some_and(|t| now >= t))
        {
            return Err(Error::Timeout("tool result acknowledgement"));
        }
        if self.create_id.as_ref().is_some_and(|(_, t)| now >= *t) {
            return Err(Error::Timeout("response creation"));
        }
        Ok(())
    }
    fn tool_joined(
        &mut self,
        joined: Option<Result<ToolResult, tokio::task::JoinError>>,
    ) -> Result<(), Error> {
        let result = joined
            .expect("polled only while tasks are pending")
            .map_err(|_| Error::Protocol("tool worker failed".into()))?;
        self.tool_result(result)
    }
    fn frame(
        &mut self,
        frame: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    ) -> Result<(), Error> {
        self.received = Instant::now();
        self.ping_sent = false;
        match frame {
            Some(Ok(Message::Text(text))) => {
                let event = serde_json::from_str(&text)
                    .map_err(|_| Error::Protocol("invalid server JSON".into()))?;
                self.server(event)
            }
            // Tungstenite queues the pong; flushing sends it ahead of ordinary media.
            Some(Ok(Message::Ping(_))) => self.enqueue(Write::Flush, true),
            Some(Ok(Message::Pong(_))) => Ok(()),
            Some(Ok(Message::Close(_)) | Err(_)) | None => Err(Error::Disconnected),
            Some(Ok(_)) => Err(Error::Protocol("expected JSON text frame".into())),
        }
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
            Command::Respond { response } => self.create_response(response)?,
            Command::Event { event } => self.forward(event)?,
            Command::Interrupt { response_id, heard } => {
                // Validate every position before issuing any cancellation or truncation.
                self.validate_heard(&response_id, &heard)?;
                if self
                    .responses
                    .get(&response_id)
                    .is_some_and(|r| r.created && !r.done)
                {
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
        // Stopping never depends on this capability; only the endpoint's context does.
        if !self.config.capabilities.truncate {
            return Ok(());
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
                let conversation = conversation(&event["response"]);
                let state = self.response(rid)?;
                if !state.created {
                    state.created = true;
                    state.conversation = state.conversation.or(conversation);
                    let in_progress = !state.done;
                    if conversation != Some(false) {
                        if in_progress {
                            self.active.insert(rid.to_owned());
                        }
                        self.create_id = None;
                    }
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
                let state = self.response(rid)?;
                state.conversation = state.conversation.or(conversation(response));
                let was_cancelled = state.cancelled;
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
        let response = self.response(rid)?;
        let (cancelled, conversation) = (response.cancelled, response.conversation);
        if cancelled || conversation == Some(false) {
            return Ok(());
        }
        if self.out_of_band_requested && conversation.is_none() {
            // Executing a call the host may have scoped out of band is not recoverable.
            return Err(Error::Protocol(
                "tool call from a response without conversation_id after an out-of-band request"
                    .into(),
            ));
        }
        let call_id = field(item, "call_id")?.to_owned();
        let name = field(item, "name")?.to_owned();
        let raw = field(item, "arguments")?;
        // Oversized arguments are never parsed or retained; the call still gets an answer.
        let parsed = (raw.len() <= self.config.limits.tool_argument_bytes).then(|| {
            serde_json::from_str::<Value>(raw).map(|mut value| {
                // Cargo features are additive across the embedding application.
                value.sort_all_objects();
                value
            })
        });
        let fingerprint = match &parsed {
            Some(Ok(value)) => value.to_string(),
            Some(Err(_)) => raw.to_owned(),
            // Rejected input is compared by raw bytes through a per-session keyed hash.
            None => format!(
                "oversized:{}:{:016x}",
                raw.len(),
                self.oversized_key.hash_one(raw)
            ),
        };
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
            state: ToolState::Admitted,
        })?;
        // A model mistake fails only its own call, never the conversation.
        let arguments = match parsed {
            None => Err("tool_arguments_too_large"),
            Some(Err(_)) => Err("invalid_arguments_json"),
            Some(Ok(arguments)) => match self.registry.validators.get(&name) {
                None => Err("unknown_tool"),
                Some(schema) if !schema.is_valid(&arguments) => Err("invalid_arguments_schema"),
                Some(_) if self.tasks.len() >= self.config.limits.concurrent_tools => {
                    Err("tool_concurrency_limit")
                }
                Some(_) => Ok(arguments),
            },
        };
        match arguments {
            Ok(arguments) => {
                let call = ToolCall {
                    call_id,
                    name,
                    arguments,
                };
                let registry = self.registry.clone();
                let limit = self.config.limits.tool_timeout;
                self.tasks.spawn(execute(registry, call, cancel, limit));
                Ok(())
            }
            Err(error) => self.tool_result(ToolResult {
                call_id,
                value: json!({ "error": error }),
            }),
        }
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
            state: ToolState::ResultSent,
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
            self.create_response(None)?;
            for id in ready {
                self.responses.get_mut(&id).expect("known").continued = true;
            }
        }
        Ok(())
    }
}

/// GA reports a null `conversation_id` for an out-of-band response.
fn conversation(response: &Value) -> Option<bool> {
    match response.get("conversation_id") {
        Some(Value::Null) => Some(false),
        Some(Value::String(_)) => Some(true),
        _ => None,
    }
}

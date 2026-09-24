#![doc = include_str!("../README.md")]
mod session;
mod tools;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, sync::Arc, time::Duration};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub use tokio_util::sync::CancellationToken as ToolCancellation;
pub use tools::{Tool, ToolCall, ToolExecutor, ToolFuture, ToolRegistry};

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("connection failed: {0}")]
    Connect(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("bounded resource exhausted: {0}")]
    Capacity(&'static str),
    #[error("session disconnected; delivery may be uncertain")]
    Disconnected,
    #[error("session initialization rejected")]
    Initialization,
    #[error("session timed out: {0}")]
    Timeout(&'static str),
    #[error("session closed")]
    Closed,
}

/// Which side sends response.create after tool results are accepted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Continuation {
    #[default]
    Client,
    Server,
}

/// Required to validate playback positions, never to estimate what was heard.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AudioFormat {
    Pcm16 { sample_rate: u32 },
    G711,
}

impl Default for AudioFormat {
    fn default() -> Self {
        Self::Pcm16 {
            sample_rate: 24_000,
        }
    }
}

impl AudioFormat {
    pub(crate) fn duration_ms(self, bytes: u64) -> u64 {
        match self {
            Self::Pcm16 { sample_rate } => {
                bytes.saturating_mul(1000) / (2 * u64::from(sample_rate))
            }
            Self::G711 => bytes / 8,
        }
    }
}

/// Explicit endpoint capabilities; no provider-name heuristics.
#[derive(Clone, Debug)]
pub struct Capabilities {
    pub audio_input: bool,
    pub image_input: bool,
    /// `conversation.item.truncate` support. Without it, stopping still cancels and invalidates
    /// playback, but the endpoint's context keeps the unheard remainder of the reply.
    pub truncate: bool,
    pub output_audio: AudioFormat,
}

impl Default for Capabilities {
    fn default() -> Self {
        Self {
            audio_input: true,
            image_input: true,
            truncate: true,
            output_audio: AudioFormat::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Limits {
    pub queue: usize,
    pub message_bytes: usize,
    /// Encoded audio cap, independent of larger image/control messages.
    pub audio_chunk_bytes: usize,
    /// Oversized model arguments answer that call with an error; the session continues.
    pub tool_argument_bytes: usize,
    pub tool_result_bytes: usize,
    /// Calls beyond this answer with an error instead of queueing; the session continues.
    pub concurrent_tools: usize,
    /// Lifetime bounds: exceeding them ends the session instead of evicting deduplication history.
    pub calls: usize,
    pub responses: usize,
    pub audio_parts: usize,
    pub connect_timeout: Duration,
    pub initialize_timeout: Duration,
    pub write_timeout: Duration,
    /// Ping halfway through silence; fail if no frame arrives before this deadline.
    pub idle_timeout: Duration,
    pub tool_timeout: Duration,
    pub acknowledgement_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            queue: 64,
            message_bytes: 8 * 1024 * 1024,
            audio_chunk_bytes: 64 * 1024,
            tool_argument_bytes: 64 * 1024,
            tool_result_bytes: 64 * 1024,
            concurrent_tools: 16,
            calls: 4096,
            responses: 8192,
            audio_parts: 4096,
            connect_timeout: Duration::from_secs(15),
            initialize_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(60),
            tool_timeout: Duration::from_secs(120),
            acknowledgement_timeout: Duration::from_secs(15),
        }
    }
}

/// Intentionally not Debug: endpoint query strings and bearer tokens can contain secrets.
pub struct Config {
    pub endpoint: String,
    pub model: Option<String>,
    pub bearer_token: Option<String>,
    /// Provider session fields (GA by default). Tools are supplied only by ToolRegistry.
    pub session: Value,
    pub continuation: Continuation,
    pub capabilities: Capabilities,
    /// Opt in to the small Frankie playback-reference/feedback extension allowlist.
    pub frankie_extensions: bool,
    pub limits: Limits,
}

impl Config {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            model: None,
            bearer_token: None,
            session: serde_json::json!({"type":"realtime"}),
            continuation: Continuation::Client,
            capabilities: Capabilities::default(),
            frankie_extensions: false,
            limits: Limits::default(),
        }
    }
}

/// A final rendered-sample position, relative to one item's audio content part.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Heard {
    pub item_id: String,
    pub content_index: u32,
    pub audio_end_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum Command {
    /// Optional Frankie playback reference/position/finished event; requires explicit configuration.
    Frankie {
        event: Value,
    },
    Text {
        text: String,
    },
    Image {
        image_url: String,
        #[serde(default)]
        text: Option<String>,
    },
    /// Base64 audio in the input format configured in the session.
    Audio {
        audio: String,
    },
    CommitAudio,
    ClearAudio,
    /// Requests a response; `response` holds optional `response.create` parameters.
    /// With `"conversation": "none"` it is out-of-band: it may run beside the default
    /// conversation's response, may declare its own tools, and its tool calls are
    /// returned to the host in `response.done`, never executed.
    Respond {
        #[serde(default)]
        response: Option<Value>,
    },
    /// Any other Realtime client event, sent as-is. Events whose state the harness owns
    /// are rejected: responses, cancellation, truncation, tool results and tools,
    /// and Frankie extensions each have their own command or configuration.
    Event {
        event: Value,
    },
    /// Stop the consumer's player first, then submit its final sample-clock positions.
    /// Speech cancellation does not cancel tools.
    Interrupt {
        response_id: String,
        #[serde(default)]
        heard: Vec<Heard>,
    },
    /// After server cancellation, report final positions when the consumer has stopped.
    PlaybackStopped {
        response_id: String,
        #[serde(default)]
        heard: Vec<Heard>,
    },
    /// Cooperative task cancellation; external side effects may already have happened.
    CancelTool {
        call_id: String,
    },
    Close,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Event {
    /// A caller mistake rejects only that command; it does not cancel the session or tools.
    CommandRejected {
        error: String,
    },
    /// All public server events, including unknown types. Suppressed stale media is omitted.
    Server {
        event: Value,
    },
    /// Clear queued playback for this response, then report PlaybackStopped with final positions.
    PlaybackClear {
        response_id: String,
    },
    Tool {
        call_id: String,
        state: ToolState,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolState {
    /// The call is committed; exactly one result follows unless the session ends first.
    Admitted,
    /// Its result or error was sent as a function_call_output item.
    ResultSent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Connecting,
    Ready,
    Closed,
    Failed(Error),
}

/// Clonable, nonblocking producer. A full command queue rejects the command unchanged in meaning:
/// it was not admitted; the caller chooses whether to stop capture or retry later.
#[derive(Clone)]
pub struct Handle {
    pub(crate) tx: mpsc::Sender<Command>,
    pub(crate) control: mpsc::Sender<Command>,
    max_bytes: usize,
    max_audio_bytes: usize,
}

impl Handle {
    pub fn send(&self, command: Command) -> Result<(), Error> {
        let oversized_audio = match &command {
            Command::Audio { audio } => audio.len() > self.max_audio_bytes,
            Command::Frankie { event } | Command::Event { event }
                if event["type"] == "input_audio_buffer.append" =>
            {
                ["audio", "playback"].iter().any(|key| {
                    event[*key]
                        .as_str()
                        .is_some_and(|s| s.len() > self.max_audio_bytes)
                })
            }
            _ => false,
        };
        if oversized_audio {
            return Err(Error::Capacity("audio chunk bytes"));
        }

        if serde_json::to_vec(&command)
            .map_err(|_| Error::Protocol("command encoding".into()))?
            .len()
            > self.max_bytes
        {
            return Err(Error::Capacity("command bytes"));
        }
        let sender = if matches!(
            command,
            Command::Interrupt { .. }
                | Command::PlaybackStopped { .. }
                | Command::CancelTool { .. }
                | Command::Close
        ) {
            &self.control
        } else {
            &self.tx
        };
        sender.try_send(command).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => Error::Capacity("command queue"),
            mpsc::error::TrySendError::Closed(_) => Error::Closed,
        })
    }
}

/// Check this watch before rendering queued media; terminal or stopped IDs invalidate all queued audio.
#[derive(Clone, Debug, Default)]
pub struct PlaybackState {
    pub stopped: HashSet<String>,
    pub terminal: bool,
}

impl PlaybackState {
    pub fn allows(&self, response_id: &str) -> bool {
        !self.terminal && !self.stopped.contains(response_id)
    }
}

pub struct Session {
    pub handle: Handle,
    pub events: mpsc::Receiver<Event>,
    /// Terminal status remains observable even when the event consumer is full.
    pub status: watch::Receiver<Status>,
    pub playback: watch::Receiver<PlaybackState>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) task: Option<tokio::task::JoinHandle<Result<(), Error>>>,
}

impl Session {
    pub async fn connect(config: Config, tools: ToolRegistry) -> Result<Self, Error> {
        session::connect(config, Arc::new(tools)).await
    }

    pub async fn finish(mut self) -> Result<(), Error> {
        self.shutdown.cancel();
        self.task
            .take()
            .expect("session task")
            .await
            .map_err(|_| Error::Closed)?
    }

    /// Waits without consuming the event stream; callers must keep draining events separately.
    pub async fn closed(&mut self) -> Result<(), Error> {
        loop {
            match self.status.borrow().clone() {
                Status::Closed => return Ok(()),
                Status::Failed(error) => return Err(error),
                _ => {}
            }
            if self.status.changed().await.is_err() {
                return Err(Error::Closed);
            }
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

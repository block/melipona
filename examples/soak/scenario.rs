use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fs, path::Path, time::Duration};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub name: String,
    #[serde(default = "rounds")]
    pub rounds: usize,
    #[serde(default = "seed")]
    pub seed: u64,
    #[serde(default = "deadline")]
    pub turn_timeout_ms: u64,
    #[serde(default = "run_deadline")]
    pub run_timeout_ms: u64,
    #[serde(default = "settle")]
    pub settle_ms: u64,
    #[serde(default)]
    pub session: Value,
    #[serde(default)]
    pub frankie: bool,
    #[serde(default)]
    pub echo_delay_ms: u64,
    #[serde(default)]
    pub context: Context,
    #[serde(default)]
    pub setup: Option<String>,
    #[serde(default)]
    pub telemetry: Option<Telemetry>,
    #[serde(default)]
    pub record_audio: bool,
    pub turns: Vec<Turn>,
}
fn rounds() -> usize {
    1
}
fn seed() -> u64 {
    1
}
fn deadline() -> u64 {
    180_000
}
fn run_deadline() -> u64 {
    3_600_000
}
fn settle() -> u64 {
    256
}

/// Explicit provider event mapping; nothing is guessed from a provider name.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Telemetry {
    pub event: String,
    /// Sum disjoint reported token categories; one pointer for an ordinary total.
    #[serde(default)]
    pub input_tokens: Vec<String>,
    #[serde(default)]
    pub cached_input_tokens: Option<String>,
    #[serde(default)]
    pub decode_tokens_per_second: Option<String>,
    #[serde(default)]
    pub peak_memory_bytes: Option<String>,
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Context {
    /// Exact UTF-8 ASCII character counts, never tokenizer estimates.
    #[serde(default)]
    pub chars_per_turn: usize,
    #[serde(default)]
    pub target_input_tokens: Option<u64>,
    /// Explicit provider error codes that mean a full context was refused.
    #[serde(default)]
    pub capacity_codes: Vec<String>,
    /// Some engines provide no code; only an explicitly configured exact message may match.
    #[serde(default)]
    pub capacity_messages: Vec<String>,
}

impl Context {
    pub fn refusal(&self, error: &Value) -> Option<String> {
        if let Some(code) = error["code"].as_str().or(error["reason"].as_str())
            && self.capacity_codes.iter().any(|c| c == code)
        {
            return Some(format!("code:{code}"));
        }
        if let Some(message) = error["message"].as_str()
            && self.capacity_messages.iter().any(|m| m == message)
        {
            return Some(format!("exact_message:{message}"));
        }
        None
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Turn {
    pub name: String,
    #[serde(default)]
    pub text: Option<String>,
    /// Headerless mono signed little-endian PCM16, 24 kHz; relative to this scenario.
    #[serde(default)]
    pub pcm: Option<String>,
    /// PNG/JPEG local file; no remote image fetches.
    #[serde(default)]
    pub image: Option<String>,
    /// Let server VAD commit/respond; otherwise commit/respond after the last PCM sample.
    #[serde(default)]
    pub server_vad: bool,
    #[serde(default)]
    pub overlap: Option<Overlap>,
    #[serde(default)]
    pub expect: Expectations,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Overlap {
    #[serde(default)]
    pub pcm: Option<String>,
    #[serde(default = "overlap_at")]
    pub after_played_ms: u64,
    /// Exercise client interruption; false tests the provider's own turn policy.
    #[serde(default)]
    pub interrupt: bool,
    /// Expected cancellation of the response active at overlap onset.
    pub expect_clear: bool,
}
fn overlap_at() -> u64 {
    1024
}

#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Expectations {
    #[serde(default)]
    pub contains: Vec<String>,
    #[serde(default)]
    pub min_words: usize,
    #[serde(default)]
    pub tool_texts: Vec<String>,
    #[serde(default)]
    pub max_stop_ms: Option<u64>,
}

impl Scenario {
    pub fn load(path: &Path) -> Result<Self> {
        let data = bounded_read(path, 1024 * 1024)?;
        let s: Self = serde_json::from_slice(&data)?;
        if s.name.is_empty()
            || s.turns.is_empty()
            || s.turns.len() > 1024
            || s.rounds == 0
            || s.rounds.saturating_mul(s.turns.len()) > 4096
            || !(1000..=3_600_000).contains(&s.turn_timeout_ms)
            || !(1000..=86_400_000).contains(&s.run_timeout_ms)
            || !(32..=10_000).contains(&s.settle_ms)
            || s.echo_delay_ms > 60_000
            || s.context.chars_per_turn > 1024 * 1024
            || s.setup.as_ref().is_some_and(|v| v.len() > 1024 * 1024)
        {
            return Err("scenario exceeds documented bounds".into());
        }
        if let Some(t) = &s.telemetry {
            let pointers = t
                .input_tokens
                .iter()
                .chain(t.cached_input_tokens.iter())
                .chain(t.decode_tokens_per_second.iter())
                .chain(t.peak_memory_bytes.iter());
            if t.event.is_empty()
                || t.event.len() > 128
                || t.input_tokens.len() > 8
                || pointers
                    .into_iter()
                    .any(|p| !p.starts_with('/') || p.len() > 256)
            {
                return Err("invalid telemetry mapping".into());
            }
        }
        for t in &s.turns {
            if t.name.is_empty()
                || (t.text.is_none() && t.pcm.is_none() && t.image.is_none())
                || t.text.as_ref().is_some_and(|v| v.len() > 1024 * 1024)
                || (t.server_vad && t.pcm.is_none())
                || t.overlap
                    .as_ref()
                    .is_some_and(|o| !o.interrupt && o.pcm.is_none())
            {
                return Err("invalid turn input/overlap".into());
            }
        }
        Ok(s)
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.turn_timeout_ms)
    }
}

pub fn bounded_read(path: &Path, max: u64) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(max + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > max {
        return Err("fixture exceeds byte limit".into());
    }
    Ok(bytes)
}

pub fn pcm(path: &Path) -> Result<Vec<u8>> {
    let bytes = bounded_read(path, 24_000 * 2 * 120)?;
    if bytes.is_empty() || bytes.len() % 2 != 0 || bytes.starts_with(b"RIFF") {
        return Err("expected nonempty raw PCM16 mono 24 kHz, at most 120 seconds".into());
    }
    Ok(bytes)
}

/// Deterministic, varied neutral data avoids a pathological all-identical-token prompt.
pub fn filler(seed: u64, turn: usize, count: usize) -> String {
    let mut state = seed.wrapping_add((turn as u64).wrapping_mul(0x9e3779b97f4a7c15));
    let mut out = String::with_capacity(count + 80);
    while out.len() < count {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!(
            "Record {}: {} items, zone {}, revision {}. ",
            out.len(),
            state % 997,
            (state >> 12) % 61,
            (state >> 20) % 10007
        ));
    }
    out.truncate(count);
    out
}

use super::scenario::Result;
use base64::{Engine, engine::general_purpose::STANDARD};
use melipona::{Heard, PlaybackState};
use serde_json::{Value, json};
use std::collections::VecDeque;

pub const RATE: u64 = 24_000;
pub const FRAME: usize = 768;

#[derive(Default)]
pub struct Player {
    parts: Vec<Part>,
    pub rendered: u64,
    pub bytes: usize,
    pub gap_samples: u64,
    was_playing: bool,
}
struct Part {
    response: String,
    item: String,
    index: u32,
    queue: VecDeque<u8>,
    played: u64,
    done: bool,
    response_done: bool,
    stopped: bool,
    notified: bool,
}

impl Player {
    pub fn add(&mut self, event: &Value) -> Result<()> {
        let response = event["response_id"].as_str().ok_or("missing response ID")?;
        let item = event["item_id"].as_str().ok_or("missing audio item ID")?;
        let index = event["content_index"].as_u64().unwrap_or(0) as u32;
        let data = STANDARD.decode(event["delta"].as_str().ok_or("missing audio")?)?;
        if data.len() % 2 != 0 || self.bytes + data.len() > RATE as usize * 2 * 120 {
            return Err("PCM playback queue exceeds 120 seconds or is misaligned".into());
        }
        let pos = self
            .parts
            .iter()
            .position(|p| p.item == item && p.index == index);
        let p = if let Some(i) = pos {
            &mut self.parts[i]
        } else {
            if self.parts.len() >= 128 {
                return Err("more than 128 audio parts in one turn".into());
            }
            self.parts.push(Part {
                response: response.into(),
                item: item.into(),
                index,
                queue: VecDeque::new(),
                played: 0,
                done: false,
                response_done: false,
                stopped: false,
                notified: false,
            });
            self.parts.last_mut().expect("inserted part")
        };
        if p.response != response || p.done {
            return Err("late or mismatched audio part".into());
        }
        if !p.stopped {
            self.bytes += data.len();
            p.queue.extend(data);
        }
        Ok(())
    }

    pub fn done(&mut self, response: &str) {
        for p in &mut self.parts {
            if p.response == response {
                p.done = true;
                p.response_done = true;
            }
        }
    }

    pub fn part_done(&mut self, item: &str, index: u32) {
        for p in &mut self.parts {
            if p.item == item && p.index == index {
                p.done = true;
            }
        }
    }

    pub fn active(&self) -> Option<&str> {
        self.parts
            .iter()
            .find(|p| !p.stopped && (!p.queue.is_empty() || !p.done))
            .map(|p| p.response.as_str())
    }

    pub fn heard(&self, response: &str) -> Vec<Heard> {
        self.parts
            .iter()
            .filter(|p| p.response == response)
            .map(|p| Heard {
                item_id: p.item.clone(),
                content_index: p.index,
                audio_end_ms: p.played * 1000 / RATE,
            })
            .collect()
    }

    pub fn clear(&mut self, response: &str) -> bool {
        let mut changed = false;
        for p in &mut self.parts {
            if p.response == response && !p.stopped {
                self.bytes -= p.queue.len();
                p.queue.clear();
                p.stopped = true;
                changed = true;
            }
        }
        changed
    }

    pub fn invalidated(&self, state: &PlaybackState) -> Vec<String> {
        let mut ids = Vec::new();
        for p in &self.parts {
            if !p.stopped && !state.allows(&p.response) && !ids.contains(&p.response) {
                ids.push(p.response.clone());
            }
        }
        ids
    }

    /// A real-time virtual sample clock, not a physical audio device measurement.
    pub fn render(&mut self) -> Vec<u8> {
        let mut frame = vec![0; FRAME * 2];
        let mut written = 0;
        for p in &mut self.parts {
            if p.stopped {
                continue;
            }
            while written < frame.len() && !p.queue.is_empty() {
                frame[written] = p.queue.pop_front().expect("nonempty queue");
                written += 1;
                p.played += u64::from(written % 2 == 0);
            }
            if written == frame.len() || !p.done {
                break;
            }
        }
        self.bytes -= written;
        self.rendered += written as u64 / 2;
        if written < frame.len() && (self.was_playing || written > 0) && self.active().is_some() {
            self.gap_samples += (frame.len() - written) as u64 / 2;
        }
        if written > 0 {
            self.was_playing = true;
        }
        frame
    }

    pub fn feedback(&mut self, position: bool) -> Vec<Value> {
        let mut events = Vec::new();
        for p in &mut self.parts {
            if p.stopped || p.notified {
                continue;
            }
            let finished = p.response_done && p.queue.is_empty();
            if finished || position {
                events.push(json!({"type":if finished {"frankie.playback.finished"} else {"frankie.playback.position"},
                    "response_id":p.response,"item_id":p.item,"audio_end_ms":p.played * 1000 / RATE,
                    "queued_samples":p.queue.len()/2}));
                p.notified = finished;
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn delta(rid: &str, item: &str, frames: usize) -> Value {
        json!({"response_id":rid,"item_id":item,"delta":STANDARD.encode(vec![1;FRAME*2*frames])})
    }
    #[test]
    fn done_is_not_heard_and_clear_is_response_scoped() {
        let mut p = Player::default();
        p.add(&delta("r1", "i1", 2)).unwrap();
        p.add(&delta("r2", "i2", 1)).unwrap();
        p.done("r1");
        p.render();
        assert_eq!(p.heard("r1")[0].audio_end_ms, 32);
        assert!(
            !p.feedback(true)
                .iter()
                .any(|e| e["type"] == "frankie.playback.finished")
        );
        assert!(p.clear("r1"));
        assert!(!p.clear("r1"));
        p.render();
        assert_eq!(p.heard("r2")[0].audio_end_ms, 32);
        assert_eq!(p.heard("r1")[0].audio_end_ms, 32);
        p.done("r2");
        assert_eq!(p.feedback(false).len(), 1);
        assert!(p.feedback(false).is_empty());
    }
    #[test]
    fn terminal_watch_invalidates_queued_audio() {
        let mut p = Player::default();
        p.add(&delta("r1", "i1", 1)).unwrap();
        let state = PlaybackState {
            terminal: true,
            ..Default::default()
        };
        for rid in p.invalidated(&state) {
            p.clear(&rid);
        }
        assert_eq!(p.render(), vec![0; FRAME * 2]);
        assert_eq!(p.rendered, 0);
    }

    #[test]
    fn partial_frame_underrun_counts_missing_samples() {
        let mut p = Player::default();
        p.add(&json!({"response_id":"r","item_id":"i","delta":STANDARD.encode(vec![1;FRAME])}))
            .unwrap();
        p.render();
        assert_eq!(p.gap_samples, FRAME as u64 / 2);
        p.done("r");
        p.render();
        assert_eq!(
            p.gap_samples,
            FRAME as u64 / 2,
            "finished tail padding is not an underrun"
        );
    }
}

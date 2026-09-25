use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use melipona::*;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{Duration, timeout},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

type Peer = WebSocketStream<TcpStream>;
struct Executor(mpsc::Sender<ToolCall>);
impl ToolExecutor for Executor {
    fn execute(&self, call: ToolCall, _: ToolCancellation) -> ToolFuture<'_> {
        if call.name == "construct_panic" {
            panic!("synthetic panic while building the future");
        }
        Box::pin(async move {
            let _ = self.0.try_send(call.clone());
            if call.name == "panic" {
                panic!("synthetic panic");
            }
            if call.name == "large" {
                return Ok(json!("x".repeat(4096)));
            }
            if call.name == "fail" {
                return Err("synthetic failure".into());
            }
            if let Some(delay) = call.arguments["delay"].as_u64() {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            Ok(json!({"value":call.arguments["value"]}))
        })
    }
}
fn registry() -> (ToolRegistry, mpsc::Receiver<ToolCall>) {
    let (tx, rx) = mpsc::channel(32);
    let definitions=["echo","fail","panic","construct_panic","large"].into_iter().map(|name|Tool{name:name.into(),description:"Synthetic test tool".into(),
        parameters:json!({"type":"object","properties":{"value":{"type":"integer"},"delay":{"type":"integer","minimum":0}},"required":["value"],"additionalProperties":false})
    }).collect();
    (
        ToolRegistry::new(definitions, Arc::new(Executor(tx))).unwrap(),
        rx,
    )
}
async fn send(peer: &mut Peer, event: Value) {
    peer.send(Message::Text(event.to_string().into()))
        .await
        .unwrap();
}
async fn recv(peer: &mut Peer) -> Value {
    loop {
        match timeout(Duration::from_secs(2), peer.next())
            .await
            .expect("peer receive timeout")
            .expect("peer closed")
            .unwrap()
        {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("unexpected frame {other:?}"),
        }
    }
}
async fn quiet(peer: &mut Peer) {
    assert!(
        timeout(Duration::from_millis(50), peer.next())
            .await
            .is_err(),
        "unexpected client write"
    );
}
async fn open(tools: ToolRegistry, configure: impl FnOnce(&mut Config)) -> (Session, Peer) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = Config::new(format!(
        "ws://{}/v1/realtime",
        listener.local_addr().unwrap()
    ));
    configure(&mut config);
    let connecting = tokio::spawn(Session::connect(config, tools));
    let (tcp, _) = listener.accept().await.unwrap();
    let mut peer = accept_async(tcp).await.unwrap();
    let session = connecting.await.unwrap().unwrap();
    assert_eq!(recv(&mut peer).await["type"], "session.update");
    (session, peer)
}
async fn ready(session: &mut Session, peer: &mut Peer) {
    send(
        peer,
        json!({"type":"session.updated","session":{"type":"realtime"}}),
    )
    .await;
    timeout(Duration::from_secs(2), async {
        while *session.status.borrow() != Status::Ready {
            session.status.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        session.events.recv().await,
        Some(Event::Server { .. })
    ));
}
async fn start(tools: ToolRegistry) -> (Session, Peer) {
    let (mut session, mut peer) = open(tools, |_| {}).await;
    ready(&mut session, &mut peer).await;
    (session, peer)
}
fn item(call: &str, args: &str, name: &str) -> Value {
    json!({"id":format!("item_{call}"),"type":"function_call","status":"completed","call_id":call,"name":name,"arguments":args})
}
async fn created(peer: &mut Peer, rid: &str) {
    send(
        peer,
        json!({"type":"response.created","response":{"id":rid,"status":"in_progress"}}),
    )
    .await;
}
async fn commit(peer: &mut Peer, rid: &str, item: Value) {
    send(
        peer,
        json!({"type":"response.output_item.done","response_id":rid,"output_index":0,"item":item}),
    )
    .await;
}
async fn done(peer: &mut Peer, rid: &str, status: &str, output: Vec<Value>) {
    send(
        peer,
        json!({"type":"response.done","response":{"id":rid,"status":status,"output":output}}),
    )
    .await;
}
async fn acknowledge(peer: &mut Peer, result: &Value) {
    send(
        peer,
        json!({"type":"conversation.item.added","item":result["item"]}),
    )
    .await;
}
async fn failed(session: &mut Session) -> Error {
    timeout(Duration::from_secs(2), async {
        loop {
            if let Status::Failed(error) = session.status.borrow().clone() {
                return error;
            }
            session.status.changed().await.unwrap();
        }
    })
    .await
    .expect("terminal failure timeout")
}
async fn event(session: &mut Session) -> Event {
    timeout(Duration::from_secs(2), session.events.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn drain_until(session: &mut Session, kind: &str) -> Value {
    loop {
        if let Event::Server { event } = event(session).await
            && event["type"] == kind
        {
            return event;
        }
    }
}

#[tokio::test]
async fn no_agent_media_unknown_events_and_text_image_inputs() {
    let (mut session, mut peer) = start(ToolRegistry::empty()).await;
    session
        .handle
        .send(Command::Text {
            text: "hello".into(),
        })
        .unwrap();
    let text = recv(&mut peer).await;
    assert_eq!(
        text["item"]["content"][0],
        json!({"type":"input_text","text":"hello"})
    );
    session
        .handle
        .send(Command::Image {
            image_url: "data:image/png;base64,iVBORw0KGgo=".into(),
            text: Some("describe".into()),
        })
        .unwrap();
    let image = recv(&mut peer).await;
    assert_eq!(image["item"]["content"][0]["type"], "input_image");
    assert_eq!(image["item"]["content"][1]["text"], "describe");
    session
        .handle
        .send(Command::Audio {
            audio: STANDARD.encode([0u8; 480]),
        })
        .unwrap();
    assert_eq!(recv(&mut peer).await["type"], "input_audio_buffer.append");
    for line in include_str!("fixtures/public-events.jsonl").lines() {
        let fixture: Value = serde_json::from_str(line).unwrap();
        send(&mut peer, fixture.clone()).await;
        assert!(matches!(event(&mut session).await,Event::Server{event} if event==fixture));
    }
    quiet(&mut peer).await;
    session.finish().await.unwrap();
}

#[tokio::test]
async fn fragments_never_execute_complete_item_starts_before_response_done() {
    let (tools, mut calls) = registry();
    let (mut session, mut peer) = start(tools).await;
    created(&mut peer, "r").await;
    send(&mut peer,json!({"type":"response.function_call_arguments.delta","response_id":"r","call_id":"c","delta":"{\"value\":"})).await;
    send(&mut peer,json!({"type":"response.function_call_arguments.done","response_id":"r","call_id":"c","name":"echo","arguments":"{\"value\":1}"})).await;
    assert!(
        timeout(Duration::from_millis(50), calls.recv())
            .await
            .is_err()
    );
    let complete = item("c", r#"{"value":1}"#, "echo");
    commit(&mut peer, "r", complete.clone()).await;
    assert_eq!(
        timeout(Duration::from_secs(1), calls.recv())
            .await
            .unwrap()
            .unwrap()
            .call_id,
        "c"
    );
    let result = recv(&mut peer).await;
    assert_eq!(result["item"]["call_id"], "c");
    assert_eq!(
        serde_json::from_str::<Value>(result["item"]["output"].as_str().unwrap()).unwrap(),
        json!({"value":1})
    );
    acknowledge(&mut peer, &result).await;
    quiet(&mut peer).await;
    done(&mut peer, "r", "completed", vec![complete]).await;
    assert_eq!(recv(&mut peer).await["type"], "response.create");
    assert!(calls.try_recv().is_err());
    quiet(&mut peer).await;
    drain_until(&mut session, "response.done").await;
    session.finish().await.unwrap();
}

#[tokio::test]
async fn fallback_parallel_tools_and_acknowledged_batch_continuation() {
    let (tools, mut calls) = registry();
    let (session, mut peer) = start(tools).await;
    done(
        &mut peer,
        "r",
        "completed",
        vec![
            item("slow", r#"{"value":1,"delay":120}"#, "echo"),
            item("fast", r#"{"value":2}"#, "echo"),
        ],
    )
    .await;
    let _ = calls.recv().await;
    let _ = calls.recv().await;
    let fast = recv(&mut peer).await;
    assert_eq!(fast["item"]["call_id"], "fast");
    acknowledge(&mut peer, &fast).await;
    let slow = recv(&mut peer).await;
    assert_eq!(slow["item"]["call_id"], "slow");
    quiet(&mut peer).await;
    acknowledge(&mut peer, &slow).await;
    assert_eq!(recv(&mut peer).await["type"], "response.create");
    acknowledge(&mut peer, &slow).await;
    quiet(&mut peer).await;
    session.finish().await.unwrap();
}

#[tokio::test]
async fn slow_tools_do_not_block_microphone_or_output_audio() {
    let (tools, mut calls) = registry();
    let (mut session, mut peer) = start(tools).await;
    created(&mut peer, "r").await;
    commit(
        &mut peer,
        "r",
        item("c", r#"{"value":1,"delay":500}"#, "echo"),
    )
    .await;
    calls.recv().await.unwrap();
    session
        .handle
        .send(Command::Audio {
            audio: STANDARD.encode([0u8; 480]),
        })
        .unwrap();
    assert_eq!(
        timeout(Duration::from_millis(200), recv(&mut peer))
            .await
            .unwrap()["type"],
        "input_audio_buffer.append"
    );
    send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"a","content_index":0,"delta":STANDARD.encode([0u8;480])})).await;
    timeout(
        Duration::from_millis(200),
        drain_until(&mut session, "response.output_audio.delta"),
    )
    .await
    .unwrap();
    session.finish().await.unwrap();
}

#[tokio::test]
async fn canonical_duplicate_suppressed_and_conflict_is_terminal() {
    let (tools, mut calls) = registry();
    let (mut session, mut peer) = start(tools).await;
    commit(
        &mut peer,
        "r",
        item("c", r#"{"value":1,"delay":0}"#, "echo"),
    )
    .await;
    calls.recv().await.unwrap();
    let _ = recv(&mut peer).await;
    commit(
        &mut peer,
        "r",
        item("c", r#"{ "delay":0, "value":1 }"#, "echo"),
    )
    .await;
    quiet(&mut peer).await;
    assert!(calls.try_recv().is_err());
    commit(&mut peer, "r", item("c", r#"{"value":2}"#, "echo")).await;
    assert!(matches!(failed(&mut session).await,Error::Protocol(s) if s.contains("conflicting")));
    assert!(session.playback.borrow().terminal);
    assert!(session.finish().await.is_err());
}

#[tokio::test]
async fn nested_argument_order_does_not_repeat_a_tool() {
    let (tx, mut calls) = mpsc::channel(2);
    let tools = ToolRegistry::new(
        vec![Tool {
            name: "echo".into(),
            description: "Synthetic nested argument test".into(),
            parameters: json!({"type":"object"}),
        }],
        Arc::new(Executor(tx)),
    )
    .unwrap();
    let (session, mut peer) = start(tools).await;
    commit(
        &mut peer,
        "r",
        item("c", r#"{"outer":[{"a":1,"b":2}]}"#, "echo"),
    )
    .await;
    calls.recv().await.unwrap();
    let result = recv(&mut peer).await;
    acknowledge(&mut peer, &result).await;
    commit(
        &mut peer,
        "r",
        item("c", r#"{"outer":[{"b":2,"a":1}]}"#, "echo"),
    )
    .await;
    quiet(&mut peer).await;
    assert!(calls.try_recv().is_err());
    session.finish().await.unwrap();
}

#[tokio::test]
async fn malformed_schema_failing_and_panicking_tools_return_results() {
    let (tools, mut calls) = registry();
    let (session, mut peer) = start(tools).await;
    for (id, args, name, expected) in [
        ("json", "{", "echo", "invalid_arguments_json"),
        (
            "schema",
            r#"{"value":"bad"}"#,
            "echo",
            "invalid_arguments_schema",
        ),
        ("unknown", r#"{"value":1}"#, "missing", "unknown_tool"),
        ("failure", r#"{"value":1}"#, "fail", "synthetic failure"),
        ("panic", r#"{"value":1}"#, "panic", "tool_panicked"),
        (
            "construct",
            r#"{"value":1}"#,
            "construct_panic",
            "tool_panicked",
        ),
    ] {
        commit(&mut peer, "r", item(id, args, name)).await;
        let result = recv(&mut peer).await;
        assert!(
            result["item"]["output"]
                .as_str()
                .unwrap()
                .contains(expected)
        );
    }
    assert_eq!(calls.recv().await.unwrap().name, "fail");
    assert_eq!(calls.recv().await.unwrap().name, "panic");
    assert!(calls.try_recv().is_err());
    session.finish().await.unwrap();
}

#[tokio::test]
async fn cancelled_or_incomplete_items_never_dispatch() {
    let (tools, mut calls) = registry();
    let (session, mut peer) = start(tools).await;
    let mut incomplete = item("bad", r#"{"value":1}"#, "echo");
    incomplete["status"] = json!("incomplete");
    commit(&mut peer, "r", incomplete).await;
    done(
        &mut peer,
        "r",
        "cancelled",
        vec![item("bad", r#"{"value":1}"#, "echo")],
    )
    .await;
    commit(&mut peer, "r", item("late", r#"{"value":2}"#, "echo")).await;
    assert!(
        timeout(Duration::from_millis(50), calls.recv())
            .await
            .is_err()
    );
    quiet(&mut peer).await;
    session.finish().await.unwrap();
}

#[tokio::test]
async fn backchannel_preserved_interrupt_uses_heard_samples_and_tools_survive() {
    let (tools, mut calls) = registry();
    let (mut session, mut peer) = start(tools).await;
    created(&mut peer, "r").await;
    commit(
        &mut peer,
        "r",
        item("c", r#"{"value":1,"delay":200}"#, "echo"),
    )
    .await;
    calls.recv().await.unwrap();
    send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"audio","content_index":0,"delta":STANDARD.encode(vec![0u8;48000])})).await;
    drain_until(&mut session, "response.output_audio.delta").await;
    send(
        &mut peer,
        json!({"type":"input_audio_buffer.speech_started","audio_start_ms":50,"item_id":"user"}),
    )
    .await;
    drain_until(&mut session, "input_audio_buffer.speech_started").await;
    assert!(session.playback.borrow().allows("r"));
    quiet(&mut peer).await;
    session
        .handle
        .send(Command::Interrupt {
            response_id: "r".into(),
            heard: vec![Heard {
                item_id: "audio".into(),
                content_index: 0,
                audio_end_ms: 123,
            }],
        })
        .unwrap();
    assert_eq!(recv(&mut peer).await["type"], "response.cancel");
    let trunc = recv(&mut peer).await;
    assert_eq!(trunc["type"], "conversation.item.truncate");
    assert_eq!(trunc["audio_end_ms"], 123);
    assert!(!session.playback.borrow().allows("r"));
    let result = recv(&mut peer).await;
    assert_eq!(result["item"]["call_id"], "c");
    assert!(!result["item"]["output"].as_str().unwrap().contains("error"));
    acknowledge(&mut peer, &result).await;
    done(&mut peer, "r", "cancelled", vec![]).await;
    send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"audio","content_index":0,"delta":"AAAA"})).await;
    send(
        &mut peer,
        json!({"type":"response.output_text.done","response_id":"r","text":"unheard"}),
    )
    .await;
    send(&mut peer, json!({"type":"synthetic.fence"})).await;
    loop {
        if let Event::Server { event } = event(&mut session).await {
            assert_ne!(event["type"], "response.output_audio.delta");
            assert_ne!(event["type"], "response.output_text.done");
            if event["type"] == "synthetic.fence" {
                break;
            }
        }
    }
    quiet(&mut peer).await;
    session.finish().await.unwrap();
}

#[tokio::test]
async fn server_cancel_invalidates_queued_audio_before_fifo_is_drained() {
    let (mut session, mut peer) = start(ToolRegistry::empty()).await;
    created(&mut peer, "r").await;
    send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"a","content_index":0,"delta":STANDARD.encode(vec![0u8;4800])})).await;
    done(&mut peer, "r", "cancelled", vec![]).await;
    timeout(Duration::from_secs(1), async {
        while session.playback.borrow().allows("r") {
            session.playback.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(session.events.len() >= 2);
    session
        .handle
        .send(Command::PlaybackStopped {
            response_id: "r".into(),
            heard: vec![Heard {
                item_id: "a".into(),
                content_index: 0,
                audio_end_ms: 50,
            }],
        })
        .unwrap();
    assert_eq!(recv(&mut peer).await["audio_end_ms"], 50);
    session.finish().await.unwrap();
}

#[tokio::test]
async fn explicit_tool_cancel_reports_uncertain_side_effects_without_speech_cancel() {
    let (tools, mut calls) = registry();
    let (session, mut peer) = start(tools).await;
    commit(
        &mut peer,
        "r",
        item("c", r#"{"value":1,"delay":1000}"#, "echo"),
    )
    .await;
    calls.recv().await.unwrap();
    session
        .handle
        .send(Command::CancelTool {
            call_id: "c".into(),
        })
        .unwrap();
    let result = recv(&mut peer).await;
    assert_eq!(result["type"], "conversation.item.create");
    assert!(
        result["item"]["output"]
            .as_str()
            .unwrap()
            .contains("external side effects")
    );
    quiet(&mut peer).await;
    session.finish().await.unwrap();
}

#[tokio::test]
async fn busy_respond_rejected_without_tearing_down_session() {
    let (mut session, mut peer) = start(ToolRegistry::empty()).await;
    created(&mut peer, "r").await;
    drain_until(&mut session, "response.created").await;
    session
        .handle
        .send(Command::Respond { response: None })
        .unwrap();
    assert!(matches!(
        event(&mut session).await,
        Event::CommandRejected { .. }
    ));
    session
        .handle
        .send(Command::Audio {
            audio: "AAAA".into(),
        })
        .unwrap();
    assert_eq!(recv(&mut peer).await["type"], "input_audio_buffer.append");
    session.finish().await.unwrap();
}

#[tokio::test]
async fn server_continuation_and_new_active_response_avoid_duplicate_create() {
    let (tools, _) = registry();
    let (mut session, mut peer) = open(tools, |c| c.continuation = Continuation::Server).await;
    ready(&mut session, &mut peer).await;
    done(
        &mut peer,
        "r",
        "completed",
        vec![item("c", r#"{"value":1}"#, "echo")],
    )
    .await;
    let result = recv(&mut peer).await;
    acknowledge(&mut peer, &result).await;
    quiet(&mut peer).await;
    session.finish().await.unwrap();

    let (tools, _) = registry();
    let (session, mut peer) = start(tools).await;
    done(
        &mut peer,
        "r",
        "completed",
        vec![item("c", r#"{"value":1}"#, "echo")],
    )
    .await;
    let result = recv(&mut peer).await;
    created(&mut peer, "new").await;
    acknowledge(&mut peer, &result).await;
    quiet(&mut peer).await;
    done(&mut peer, "new", "completed", vec![]).await;
    assert_eq!(recv(&mut peer).await["type"], "response.create");
    session.finish().await.unwrap();
}

#[tokio::test]
async fn result_rejection_and_missing_ack_fail_without_retry() {
    for reject in [true, false] {
        let (tools, _) = registry();
        let (mut session, mut peer) = open(tools, |c| {
            c.limits.acknowledgement_timeout = Duration::from_millis(100)
        })
        .await;
        ready(&mut session, &mut peer).await;
        done(
            &mut peer,
            "r",
            "completed",
            vec![item("c", r#"{"value":1}"#, "echo")],
        )
        .await;
        let result = recv(&mut peer).await;
        if reject {
            send(&mut peer,json!({"type":"error","error":{"type":"invalid_request_error","event_id":result["event_id"],"message":"synthetic rejection"}})).await;
        }
        let error = failed(&mut session).await;
        assert!(matches!(error, Error::Protocol(_) | Error::Timeout(_)));
        assert!(session.finish().await.is_err());
    }
}

#[tokio::test]
async fn bounded_command_queue_is_nonblocking_and_consumer_overflow_is_terminal() {
    let (mut session, mut peer) = open(ToolRegistry::empty(), |c| c.limits.queue = 2).await;
    session
        .handle
        .send(Command::Text { text: "one".into() })
        .unwrap();
    session
        .handle
        .send(Command::Text { text: "two".into() })
        .unwrap();
    assert_eq!(
        session.handle.send(Command::Text {
            text: "three".into()
        }),
        Err(Error::Capacity("command queue"))
    );
    ready(&mut session, &mut peer).await;
    let _ = recv(&mut peer).await;
    let _ = recv(&mut peer).await;
    for n in 0..3 {
        send(&mut peer, json!({"type":"synthetic.event","n":n})).await;
    }
    assert_eq!(
        failed(&mut session).await,
        Error::Capacity("consumer event queue")
    );
    assert!(session.playback.borrow().terminal);
    assert!(session.finish().await.is_err());
}

#[tokio::test]
async fn lifetime_ledgers_end_the_session_at_their_cap() {
    for scenario in ["calls", "responses", "audio"] {
        let (tools, _) = registry();
        let (mut session, mut peer) = open(tools, |c| match scenario {
            "calls" => c.limits.calls = 1,
            "responses" => c.limits.responses = 1,
            "audio" => c.limits.audio_parts = 1,
            _ => unreachable!(),
        })
        .await;
        ready(&mut session, &mut peer).await;
        match scenario {
            "responses" => {
                created(&mut peer, "r1").await;
                created(&mut peer, "r2").await;
            }
            "audio" => {
                for item in ["a", "b"] {
                    send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":item,"content_index":0,"delta":"AAAA"})).await;
                }
            }
            _ => {
                commit(&mut peer, "r", item("c1", r#"{"value":1}"#, "echo")).await;
                commit(&mut peer, "r", item("c2", r#"{"value":2}"#, "echo")).await;
            }
        }
        assert!(
            matches!(failed(&mut session).await, Error::Capacity(_)),
            "{scenario}"
        );
        assert!(session.finish().await.is_err());
    }
}

#[tokio::test]
async fn model_overload_fails_only_the_offending_call() {
    let (tools, mut calls) = registry();
    let (mut session, mut peer) = open(tools, |c| {
        c.limits.tool_argument_bytes = 32;
        c.limits.concurrent_tools = 1;
    })
    .await;
    ready(&mut session, &mut peer).await;
    created(&mut peer, "r").await;
    commit(
        &mut peer,
        "r",
        item("slow", r#"{"value":1,"delay":300}"#, "echo"),
    )
    .await;
    assert_eq!(calls.recv().await.unwrap().call_id, "slow");
    let big = format!(r#"{{"value":1,"pad":"{}"}}"#, "x".repeat(64));
    for (id, args, expected) in [
        ("busy", r#"{"value":2}"#, "tool_concurrency_limit"),
        ("big", big.as_str(), "tool_arguments_too_large"),
    ] {
        commit(&mut peer, "r", item(id, args, "echo")).await;
        let result = recv(&mut peer).await;
        assert_eq!(result["item"]["call_id"], id);
        assert_eq!(
            result["item"]["output"],
            json!({ "error": expected }).to_string()
        );
        acknowledge(&mut peer, &result).await;
    }
    // The repeated oversized call is deduplicated, not answered twice.
    commit(&mut peer, "r", item("big", &big, "echo")).await;
    let slow = recv(&mut peer).await;
    assert_eq!(slow["item"]["call_id"], "slow");
    acknowledge(&mut peer, &slow).await;
    assert!(calls.try_recv().is_err(), "rejected calls never execute");
    let output = vec![
        item("slow", r#"{"value":1,"delay":300}"#, "echo"),
        item("busy", r#"{"value":2}"#, "echo"),
        item("big", &big, "echo"),
    ];
    done(&mut peer, "r", "completed", output).await;
    assert_eq!(recv(&mut peer).await["type"], "response.create");
    assert_eq!(*session.status.borrow(), Status::Ready);
    session.finish().await.unwrap();
}

#[tokio::test]
async fn oversized_duplicate_with_different_bytes_is_a_conflict() {
    let (tools, mut calls) = registry();
    let (mut session, mut peer) = open(tools, |c| c.limits.tool_argument_bytes = 16).await;
    ready(&mut session, &mut peer).await;
    created(&mut peer, "r").await;
    let (first, second) = (r#"{"value":1111111111}"#, r#"{"value":2222222222}"#);
    assert_eq!(first.len(), second.len());
    commit(&mut peer, "r", item("c", first, "echo")).await;
    let result = recv(&mut peer).await;
    assert_eq!(
        result["item"]["output"],
        json!({ "error": "tool_arguments_too_large" }).to_string()
    );
    acknowledge(&mut peer, &result).await;
    commit(&mut peer, "r", item("c", second, "echo")).await;
    let failed = session.status.wait_for(|s| matches!(s, Status::Failed(_)));
    timeout(Duration::from_secs(3), failed)
        .await
        .expect("changed bytes must fail the session")
        .unwrap();
    assert!(matches!(
        session.status.borrow().clone(),
        Status::Failed(Error::Protocol(e)) if e.contains("conflicting duplicate")
    ));
    assert!(calls.try_recv().is_err());
    assert!(session.finish().await.is_err());
}

#[tokio::test]
async fn stopping_audio_never_depends_on_truncation_support() {
    for explicit in [true, false] {
        let (mut session, mut peer) =
            open(ToolRegistry::empty(), |c| c.capabilities.truncate = false).await;
        ready(&mut session, &mut peer).await;
        created(&mut peer, "r").await;
        send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"a","content_index":0,"delta":STANDARD.encode([0u8;4800])})).await;
        drain_until(&mut session, "response.output_audio.delta").await;
        let heard = vec![Heard {
            item_id: "a".into(),
            content_index: 0,
            audio_end_ms: 40,
        }];
        let response_id = "r".to_owned();
        session
            .handle
            .send(if explicit {
                Command::Interrupt { response_id, heard }
            } else {
                Command::PlaybackStopped { response_id, heard }
            })
            .unwrap();
        if explicit {
            assert_eq!(recv(&mut peer).await["type"], "response.cancel");
        }
        assert!(matches!(
            event(&mut session).await,
            Event::PlaybackClear { .. }
        ));
        assert!(!session.playback.borrow().allows("r"));
        quiet(&mut peer).await; // no truncate the endpoint cannot accept
        // Later audio from the stopped response never reaches the host.
        send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"a","content_index":0,"delta":STANDARD.encode([0u8;480])})).await;
        send(&mut peer, json!({"type":"marker"})).await;
        assert!(matches!(
            event(&mut session).await,
            Event::Server { event } if event["type"] == "marker"
        ));
        // Heard positions are still validated.
        session
            .handle
            .send(Command::PlaybackStopped {
                response_id: "r".into(),
                heard: vec![Heard {
                    item_id: "a".into(),
                    content_index: 0,
                    audio_end_ms: 999,
                }],
            })
            .unwrap();
        assert!(matches!(
            event(&mut session).await,
            Event::CommandRejected { .. }
        ));
        session.finish().await.unwrap();
    }
}

#[tokio::test]
async fn initialization_error_timeout_and_disconnect_are_observable() {
    for scenario in ["error", "timeout", "disconnect"] {
        let (mut session, mut peer) = open(ToolRegistry::empty(), |c| {
            c.limits.initialize_timeout = Duration::from_millis(100)
        })
        .await;
        match scenario {
            "error"=>send(&mut peer,json!({"type":"error","error":{"type":"invalid_request_error","message":"synthetic session error"}})).await,
            "disconnect"=>{peer.close(None).await.unwrap();},
            _=>{},
        }
        assert!(matches!(
            failed(&mut session).await,
            Error::Initialization | Error::Timeout(_) | Error::Disconnected
        ));
        assert!(session.playback.borrow().terminal);
        assert!(session.finish().await.is_err());
    }
}

#[tokio::test]
async fn idle_peer_ping_is_answered_and_drop_closes_connection() {
    let (session, mut peer) = start(ToolRegistry::empty()).await;
    peer.send(Message::Ping(vec![1, 2, 3].into()))
        .await
        .unwrap();
    assert!(
        matches!(timeout(Duration::from_secs(1),peer.next()).await.unwrap().unwrap().unwrap(),Message::Pong(v) if v.as_ref()==[1,2,3])
    );
    drop(session);
    let next = timeout(Duration::from_secs(1), peer.next()).await.unwrap();
    assert!(next.is_none() || next.unwrap().is_err());
}

#[tokio::test]
async fn insecure_remote_and_bad_auth_rejected_without_secret_leak() {
    for endpoint in [
        "ws://example.com/realtime",
        "wss://user:secret@example.com/path",
        "not-a-url",
    ] {
        let result = Session::connect(Config::new(endpoint), ToolRegistry::empty()).await;
        assert!(matches!(result, Err(Error::Config(_))));
    }
    let mut config = Config::new("wss://example.com/realtime");
    config.bearer_token = Some("supersecret\nheader".into());
    let error = Session::connect(config, ToolRegistry::empty())
        .await
        .err()
        .unwrap();
    assert!(!error.to_string().contains("supersecret"));
}

#[tokio::test]
async fn tool_timeout_and_result_size_are_bounded() {
    let (tools, _) = registry();
    let (mut session, mut peer) =
        open(tools, |c| c.limits.tool_timeout = Duration::from_millis(30)).await;
    ready(&mut session, &mut peer).await;
    commit(
        &mut peer,
        "r",
        item("c", r#"{"value":1,"delay":500}"#, "echo"),
    )
    .await;
    assert!(
        recv(&mut peer).await["item"]["output"]
            .as_str()
            .unwrap()
            .contains("tool_timeout")
    );
    session.finish().await.unwrap();
}

#[tokio::test]
async fn invalid_heard_position_rejected_and_text_only_cancel_needs_no_truncate() {
    let (mut session, mut peer) = start(ToolRegistry::empty()).await;
    created(&mut peer, "r").await;
    send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"a","content_index":0,"delta":STANDARD.encode([0u8;480])})).await;
    drain_until(&mut session, "response.output_audio.delta").await;
    session
        .handle
        .send(Command::Interrupt {
            response_id: "r".into(),
            heard: vec![Heard {
                item_id: "a".into(),
                content_index: 0,
                audio_end_ms: 999,
            }],
        })
        .unwrap();
    assert!(matches!(
        event(&mut session).await,
        Event::CommandRejected { .. }
    ));
    assert!(session.playback.borrow().allows("r"));
    quiet(&mut peer).await;
    session.finish().await.unwrap();

    let (mut session, mut peer) =
        open(ToolRegistry::empty(), |c| c.capabilities.truncate = false).await;
    ready(&mut session, &mut peer).await;
    created(&mut peer, "r").await;
    drain_until(&mut session, "response.created").await;
    session
        .handle
        .send(Command::Interrupt {
            response_id: "r".into(),
            heard: vec![],
        })
        .unwrap();
    assert_eq!(recv(&mut peer).await["type"], "response.cancel");
    assert!(matches!(
        event(&mut session).await,
        Event::PlaybackClear { .. }
    ));
    session.finish().await.unwrap();
}

#[tokio::test]
async fn completed_audio_can_be_interrupted_without_canceling_finished_response() {
    let (mut session, mut peer) = start(ToolRegistry::empty()).await;
    created(&mut peer, "r").await;
    send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"a","content_index":0,"delta":STANDARD.encode([0u8;4800])})).await;
    done(&mut peer, "r", "completed", vec![]).await;
    drain_until(&mut session, "response.done").await;
    session
        .handle
        .send(Command::Interrupt {
            response_id: "r".into(),
            heard: vec![Heard {
                item_id: "a".into(),
                content_index: 0,
                audio_end_ms: 70,
            }],
        })
        .unwrap();
    let event = recv(&mut peer).await;
    assert_eq!(event["type"], "conversation.item.truncate");
    assert_eq!(event["audio_end_ms"], 70);
    quiet(&mut peer).await;
    session.finish().await.unwrap();
}

#[tokio::test]
async fn negotiated_audio_format_and_per_part_history_are_respected() {
    let (mut session, mut peer) = start(ToolRegistry::empty()).await;
    send(&mut peer,json!({"type":"session.updated","session":{"audio":{"output":{"format":{"type":"audio/pcmu"}}}}})).await;
    created(&mut peer, "r").await;
    send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"a","content_index":0,"delta":STANDARD.encode([0u8;800])})).await;
    // Old G711 bytes retain 100ms duration after future output switches to 24kHz PCM.
    send(&mut peer,json!({"type":"session.updated","session":{"audio":{"output":{"format":{"type":"audio/pcm","rate":24000}}}}})).await;
    done(&mut peer, "r", "completed", vec![]).await;
    drain_until(&mut session, "response.done").await;
    session
        .handle
        .send(Command::Interrupt {
            response_id: "r".into(),
            heard: vec![Heard {
                item_id: "a".into(),
                content_index: 0,
                audio_end_ms: 95,
            }],
        })
        .unwrap();
    assert_eq!(recv(&mut peer).await["audio_end_ms"], 95);
    session.finish().await.unwrap();
}

#[tokio::test]
async fn a_responses_format_is_fixed_at_creation() {
    let (mut session, mut peer) = start(ToolRegistry::empty()).await;
    let pcmu = json!({"type":"session.updated","session":{"audio":{"output":{"format":{"type":"audio/pcmu"}}}}});
    let delta = |rid: &str, item: &str| json!({"type":"response.output_audio.delta","response_id":rid,"item_id":item,"content_index":0,"delta":STANDARD.encode([0u8; 4800])});
    let interrupt = |ms| Command::Interrupt {
        response_id: "r".into(),
        heard: vec![Heard {
            item_id: "a".into(),
            content_index: 0,
            audio_end_ms: ms,
        }],
    };
    // Created under 24 kHz PCM: a later default, before or during its audio, is not its.
    created(&mut peer, "r").await;
    send(&mut peer, pcmu.clone()).await;
    send(&mut peer, delta("r", "a")).await;
    send(&mut peer, pcmu).await;
    send(&mut peer, delta("r", "a")).await;
    drain_until(&mut session, "response.output_audio.delta").await;
    drain_until(&mut session, "response.output_audio.delta").await;
    // 9600 bytes are 200 ms of PCM16, not 1200 ms of G.711.
    session.handle.send(interrupt(500)).unwrap();
    assert!(rejected(&mut session).await.contains("heard position"));
    session.handle.send(interrupt(200)).unwrap();
    assert_eq!(recv(&mut peer).await["type"], "response.cancel");
    assert_eq!(recv(&mut peer).await["audio_end_ms"], 200);
    // Audio before creation takes the current default; creation cannot change it.
    send(&mut peer, delta("s", "b")).await;
    send(&mut peer,json!({"type":"response.created","response":{"id":"s","audio":{"output":{"format":{"type":"audio/pcm"}}}}})).await;
    send(&mut peer, delta("s", "b")).await;
    assert!(
        matches!(failed(&mut session).await,Error::Protocol(s) if s.contains("format conflict"))
    );
    assert!(session.finish().await.is_err());
}

#[tokio::test]
async fn cross_response_duplicate_call_id_is_a_conflict_not_another_execution() {
    let (tools, mut calls) = registry();
    let (mut session, mut peer) = start(tools).await;
    commit(&mut peer, "r1", item("c", r#"{"value":1}"#, "echo")).await;
    calls.recv().await.unwrap();
    let _ = recv(&mut peer).await;
    commit(&mut peer, "r2", item("c", r#"{"value":1}"#, "echo")).await;
    assert!(matches!(failed(&mut session).await,Error::Protocol(s) if s.contains("conflicting")));
    assert!(calls.try_recv().is_err());
    assert!(session.finish().await.is_err());
}

#[tokio::test]
async fn close_has_independent_admission_when_audio_queue_is_full_before_ready() {
    let (mut session, _peer) = open(ToolRegistry::empty(), |c| c.limits.queue = 1).await;
    session
        .handle
        .send(Command::Audio {
            audio: "AAAA".into(),
        })
        .unwrap();
    assert!(matches!(
        session.handle.send(Command::Audio {
            audio: "AAAA".into()
        }),
        Err(Error::Capacity(_))
    ));
    session.handle.send(Command::Close).unwrap();
    timeout(Duration::from_secs(1), session.closed())
        .await
        .unwrap()
        .unwrap();
    assert!(session.playback.borrow().terminal);
    session.finish().await.unwrap();
}

#[tokio::test]
async fn silent_half_open_peer_is_detected_without_an_active_response() {
    let (mut session, mut peer) = open(ToolRegistry::empty(), |c| {
        c.limits.idle_timeout = Duration::from_millis(150)
    })
    .await;
    ready(&mut session, &mut peer).await;
    // Keep the TCP connection open without reading/responding to WebSocket Ping.
    assert_eq!(failed(&mut session).await, Error::Timeout("peer liveness"));
    assert!(session.playback.borrow().terminal);
    assert!(session.finish().await.is_err());
    drop(peer);
}

#[tokio::test]
async fn stalled_socket_writer_terminates_without_double_poll_panic() {
    let (mut session, mut peer) = open(ToolRegistry::empty(), |c| {
        c.limits.queue = 1024;
        c.limits.write_timeout = Duration::from_millis(40);
    })
    .await;
    ready(&mut session, &mut peer).await;
    let audio = STANDARD.encode(vec![0u8; 48 * 1024]);
    for _ in 0..512 {
        session
            .handle
            .send(Command::Audio {
                audio: audio.clone(),
            })
            .unwrap();
    }
    assert_eq!(failed(&mut session).await, Error::Timeout("write"));
    assert!(session.playback.borrow().terminal);
    assert_eq!(session.finish().await, Err(Error::Timeout("write")));
    drop(peer);
}

#[tokio::test]
async fn wss_initializes_crypto_and_rejects_untrusted_certificate() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(cert.signing_key.serialize_der().into());
    let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert.cert.der().clone()], key)
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "wss://localhost:{}/realtime",
        listener.local_addr().unwrap().port()
    );
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        tokio_rustls::TlsAcceptor::from(Arc::new(tls))
            .accept(socket)
            .await
    });
    let error = Session::connect(Config::new(url), ToolRegistry::empty())
        .await
        .err()
        .unwrap();
    assert!(matches!(error, Error::Connect(_)));
    assert!(server.await.unwrap().is_err());
}

#[tokio::test]
#[allow(clippy::result_large_err)] // tungstenite's callback fixes this external error type.
async fn handshake_auth_failure_is_redacted_and_model_query_encoded() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = Config::new(format!(
        "ws://{}/realtime?model=old",
        listener.local_addr().unwrap()
    ));
    config.model = Some("model / with spaces".into());
    config.bearer_token = Some("synthetic-secret".into());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        tokio_tungstenite::accept_hdr_async(
            socket,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, _response| {
                assert_eq!(
                    request.headers()["authorization"],
                    "Bearer synthetic-secret"
                );
                let url = url::Url::parse(&format!("ws://localhost{}", request.uri())).unwrap();
                let models: Vec<_> = url.query_pairs().filter(|(k, _)| k == "model").collect();
                assert_eq!(models.len(), 1);
                assert_eq!(models[0].1, "model / with spaces");
                Err(tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(401)
                    .body(Some("synthetic-secret".into()))
                    .unwrap())
            },
        )
        .await
    });
    let error = Session::connect(config, ToolRegistry::empty())
        .await
        .err()
        .unwrap();
    assert_eq!(error, Error::Connect("HTTP 401".into()));
    assert!(server.await.unwrap().is_err());
}

#[test]
fn external_schema_retrieval_is_disabled() {
    let (tx, _) = mpsc::channel(1);
    for reference in ["https://example.com/schema", "file:///nonexistent/schema"] {
        let result = ToolRegistry::new(
            vec![Tool {
                name: "test".into(),
                description: "test".into(),
                parameters: json!({"$ref":reference}),
            }],
            Arc::new(Executor(tx.clone())),
        );
        assert!(matches!(result, Err(Error::Config(_))));
    }
}

#[tokio::test]
async fn incomplete_response_preserves_valid_audio_without_tool_continuation() {
    let (tools, _) = registry();
    let (mut session, mut peer) = start(tools).await;
    created(&mut peer, "r").await;
    send(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"a","content_index":0,"delta":"AAAA"})).await;
    commit(&mut peer, "r", item("c", r#"{"value":1}"#, "echo")).await;
    let result = recv(&mut peer).await;
    done(&mut peer, "r", "incomplete", vec![]).await;
    acknowledge(&mut peer, &result).await;
    drain_until(&mut session, "response.done").await;
    assert!(session.playback.borrow().allows("r"));
    quiet(&mut peer).await;
    session.finish().await.unwrap();
}

#[tokio::test]
async fn pcm_default_rate_and_opt_in_frankie_feedback() {
    let (mut session, mut peer) =
        open(ToolRegistry::empty(), |c| c.frankie_extensions = true).await;
    send(&mut peer,json!({"type":"session.updated","session":{"audio":{"output":{"format":{"type":"audio/pcm"}}},"frankie":{"playback_feedback":true}}})).await;
    drain_until(&mut session, "session.updated").await;
    session
        .handle
        .send(Command::Frankie {
            event: json!({"type":"input_audio_buffer.append","audio":"AAAA","playback":"AAAA"}),
        })
        .unwrap();
    assert_eq!(recv(&mut peer).await["playback"], "AAAA");
    session
        .handle
        .send(Command::Frankie {
            event: json!({"type":"response.create"}),
        })
        .unwrap();
    assert!(matches!(
        event(&mut session).await,
        Event::CommandRejected { .. }
    ));
    session.finish().await.unwrap();

    let (mut session, mut peer) = start(ToolRegistry::empty()).await;
    session
        .handle
        .send(Command::Frankie {
            event: json!({"type":"input_audio_buffer.append","audio":"AAAA","playback":"AAAA"}),
        })
        .unwrap();
    assert!(matches!(
        event(&mut session).await,
        Event::CommandRejected { .. }
    ));
    quiet(&mut peer).await;
    session.finish().await.unwrap();
}

#[tokio::test]
async fn audio_and_result_sizes_are_independently_bounded() {
    let (tools, _) = registry();
    let (mut session, mut peer) = open(tools, |c| {
        c.limits.audio_chunk_bytes = 64;
        c.limits.tool_result_bytes = 128;
    })
    .await;
    ready(&mut session, &mut peer).await;
    assert_eq!(
        session.handle.send(Command::Audio {
            audio: "A".repeat(68)
        }),
        Err(Error::Capacity("audio chunk bytes"))
    );
    commit(&mut peer, "r", item("c", r#"{"value":1}"#, "large")).await;
    assert!(
        recv(&mut peer).await["item"]["output"]
            .as_str()
            .unwrap()
            .contains("tool_result_too_large")
    );
    session.finish().await.unwrap();
}

#[tokio::test]
async fn interrupt_has_independent_admission_behind_queued_audio() {
    let (mut session, mut peer) = open(ToolRegistry::empty(), |c| c.limits.queue = 4).await;
    ready(&mut session, &mut peer).await;
    created(&mut peer, "r").await;
    drain_until(&mut session, "response.created").await;
    // On this current-thread runtime no coordinator runs between these nonawaiting sends.
    for _ in 0..4 {
        session
            .handle
            .send(Command::Audio {
                audio: "AAAA".into(),
            })
            .unwrap();
    }
    assert!(
        session
            .handle
            .send(Command::Audio {
                audio: "AAAA".into()
            })
            .is_err()
    );
    session
        .handle
        .send(Command::Interrupt {
            response_id: "r".into(),
            heard: vec![],
        })
        .unwrap();
    assert_eq!(recv(&mut peer).await["type"], "response.cancel");
    assert!(!session.playback.borrow().allows("r"));
    session.finish().await.unwrap();
}

async fn rejected(session: &mut Session) -> String {
    loop {
        match event(session).await {
            Event::CommandRejected { error } => return error,
            Event::Server { .. } => {}
            other => panic!("unexpected event {other:?}"),
        }
    }
}

#[tokio::test]
async fn respond_parameters_and_parallel_out_of_band_responses() {
    let bare: Command = serde_json::from_str(r#"{"command":"respond"}"#).unwrap();
    assert!(matches!(bare, Command::Respond { response: None }));
    let (tools, mut calls) = registry();
    let (mut session, mut peer) = start(tools).await;
    let respond = |response: Value| Command::Respond {
        response: Some(response),
    };
    session
        .handle
        .send(respond(json!({"instructions":"Be brief."})))
        .unwrap();
    let create = recv(&mut peer).await;
    assert_eq!(create["type"], "response.create");
    assert_eq!(create["response"]["instructions"], "Be brief.");
    send(&mut peer,json!({"type":"response.created","response":{"id":"r","status":"in_progress","conversation_id":"conv_1"}})).await;
    drain_until(&mut session, "response.created").await;

    // The default conversation admits one response; an out-of-band one runs beside it.
    session
        .handle
        .send(Command::Respond { response: None })
        .unwrap();
    assert!(rejected(&mut session).await.contains("already active"));
    let tool = json!({"type":"function","name":"classify","parameters":{"type":"object"}});
    session
        .handle
        .send(respond(json!({"tools":[tool.clone()]})))
        .unwrap();
    assert!(rejected(&mut session).await.contains("tool registry"));
    session
        .handle
        .send(respond(
            json!({"conversation":"none","metadata":{"topic":"x"},"tools":[tool]}),
        ))
        .unwrap();
    let side = recv(&mut peer).await;
    assert_eq!(side["response"]["conversation"], "none");
    assert_eq!(side["response"]["tools"][0]["name"], "classify");

    // Its calls go back to the host; nothing executes and no result is written.
    send(&mut peer,json!({"type":"response.created","response":{"id":"o","status":"in_progress","conversation_id":null}})).await;
    let call = item("c", r#"{"value":1}"#, "echo");
    commit(&mut peer, "o", call.clone()).await;
    send(&mut peer,json!({"type":"response.done","response":{"id":"o","status":"completed","conversation_id":null,"output":[call]}})).await;
    drain_until(&mut session, "response.done").await;
    quiet(&mut peer).await;
    assert!(calls.try_recv().is_err());

    // Interrupting an out-of-band response still cancels it.
    send(&mut peer,json!({"type":"response.created","response":{"id":"o2","status":"in_progress","conversation_id":null}})).await;
    drain_until(&mut session, "response.created").await;
    session
        .handle
        .send(Command::Interrupt {
            response_id: "o2".into(),
            heard: vec![],
        })
        .unwrap();
    let cancel = recv(&mut peer).await;
    assert_eq!(cancel["type"], "response.cancel");
    assert_eq!(cancel["response_id"], "o2");

    // The default conversation's calls still execute beside out-of-band responses.
    commit(&mut peer, "r", item("d", r#"{"value":2}"#, "echo")).await;
    assert_eq!(calls.recv().await.unwrap().call_id, "d");
    assert_eq!(*session.status.borrow(), Status::Ready);
    session.finish().await.unwrap();
}

#[tokio::test]
async fn unattributable_calls_after_out_of_band_requests_never_execute() {
    let (tools, mut calls) = registry();
    let (mut session, mut peer) = start(tools).await;
    session
        .handle
        .send(Command::Respond {
            response: Some(json!({"conversation":"none"})),
        })
        .unwrap();
    recv(&mut peer).await;
    created(&mut peer, "o").await; // No conversation_id: ownership is unknown.
    commit(&mut peer, "o", item("c", r#"{"value":1}"#, "echo")).await;
    assert!(matches!(
        failed(&mut session).await,
        Error::Protocol(e) if e.contains("without conversation_id")
    ));
    assert!(calls.try_recv().is_err());
}

#[tokio::test]
async fn events_are_forwarded_unless_the_harness_owns_their_state() {
    let (mut session, mut peer) = start(ToolRegistry::empty()).await;
    for event in [
        json!({"type":"session.update","session":{"type":"realtime","instructions":"Speak French."}}),
        json!({"type":"conversation.item.create","previous_item_id":"root","item":{"type":"message","role":"system","content":[{"type":"input_text","text":"Be kind."}]}}),
        json!({"type":"conversation.item.create","item":{"type":"mcp_approval_response","approval_request_id":"a","approve":true}}),
        json!({"type":"conversation.item.delete","item_id":"i"}),
        json!({"type":"conversation.item.retrieve","item_id":"i"}),
    ] {
        session
            .handle
            .send(Command::Event {
                event: event.clone(),
            })
            .unwrap();
        let mut sent = recv(&mut peer).await;
        assert!(sent["event_id"].is_string());
        sent.as_object_mut().unwrap().remove("event_id");
        assert_eq!(sent, event);
    }
    for (event, owner) in [
        (json!({"type":"response.create"}), "respond"),
        (json!({"type":"response.cancel"}), "interrupt"),
        (
            json!({"type":"conversation.item.truncate","item_id":"i","content_index":0,"audio_end_ms":0}),
            "interrupt",
        ),
        (
            json!({"type":"conversation.item.create","item":{"type":"function_call_output","call_id":"c","output":"{}"}}),
            "tool registry",
        ),
        (
            json!({"type":"session.update","session":{"type":"realtime","tools":[]}}),
            "tool registry",
        ),
        (
            json!({"type":"frankie.playback.finished","item_id":"i","response_id":"r","audio_end_ms":0}),
            "frankie",
        ),
        (json!({"item_id":"i"}), "type"),
    ] {
        session.handle.send(Command::Event { event }).unwrap();
        assert!(rejected(&mut session).await.contains(owner));
    }
    quiet(&mut peer).await;
    assert_eq!(*session.status.borrow(), Status::Ready);
    session.finish().await.unwrap();
}

#[tokio::test]
async fn requested_output_format_governs_that_responses_playback_positions() {
    let (mut session, mut peer) = start(ToolRegistry::empty()).await;
    let g711 = json!({"audio":{"output":{"format":{"type":"audio/pcmu"}}}});
    let second = STANDARD.encode([0u8; 8000]); // One second of G.711; 167 ms as PCM16.
    let interrupt = |rid: &str| Command::Interrupt {
        response_id: rid.into(),
        heard: vec![Heard {
            item_id: format!("a_{rid}"),
            content_index: 0,
            audio_end_ms: 500,
        }],
    };
    let respond = |response: Value| Command::Respond {
        response: Some(response),
    };
    // A default-conversation request, then an out-of-band one: without a reported
    // format, each response takes the one its request asked for. A reported one wins.
    let mut out_of_band = g711.clone();
    out_of_band["conversation"] = json!("none");
    let reported = json!({"conversation_id":"conv","audio":g711["audio"]});
    for (request, mut response, rid) in [
        (g711, json!({"conversation_id":"conv"}), "r"),
        (out_of_band, json!({"conversation_id":null}), "o"),
        (json!({}), reported, "p"),
    ] {
        session.handle.send(respond(request)).unwrap();
        recv(&mut peer).await;
        response["id"] = json!(rid);
        send(
            &mut peer,
            json!({"type":"response.created","response":response}),
        )
        .await;
        send(&mut peer, json!({"type":"response.output_audio.delta","response_id":rid,"item_id":format!("a_{rid}"),"content_index":0,"delta":second})).await;
        drain_until(&mut session, "response.output_audio.delta").await;
        session.handle.send(interrupt(rid)).unwrap();
        assert_eq!(recv(&mut peer).await["type"], "response.cancel");
        let truncate = recv(&mut peer).await;
        assert_eq!(truncate["type"], "conversation.item.truncate");
        assert_eq!(truncate["audio_end_ms"], 500);
        done(&mut peer, rid, "cancelled", vec![]).await;
        drain_until(&mut session, "response.done").await;
    }
    session.finish().await.unwrap();
}

#[tokio::test]
async fn unacknowledged_out_of_band_requests_time_out() {
    let (mut session, mut peer) = open(ToolRegistry::empty(), |c| {
        c.limits.acknowledgement_timeout = Duration::from_millis(100)
    })
    .await;
    ready(&mut session, &mut peer).await;
    let respond = || Command::Respond {
        response: Some(json!({"conversation":"none"})),
    };
    // A created response names no request, so one awaits acknowledgement at a time.
    session.handle.send(respond()).unwrap();
    let first = recv(&mut peer).await;
    session.handle.send(respond()).unwrap();
    assert!(
        rejected(&mut session)
            .await
            .contains("awaiting acknowledgement")
    );
    // Rejected and created requests are settled.
    send(&mut peer, json!({"type":"error","error":{"type":"invalid_request_error","message":"no","event_id":first["event_id"]}})).await;
    drain_until(&mut session, "error").await;
    session.handle.send(respond()).unwrap();
    recv(&mut peer).await;
    send(&mut peer, json!({"type":"response.created","response":{"id":"o","status":"in_progress","conversation_id":null}})).await;
    drain_until(&mut session, "response.created").await;
    // A provider may omit conversation_id; with no in-band request pending, the
    // creation can only answer the out-of-band one.
    session.handle.send(respond()).unwrap();
    recv(&mut peer).await;
    created(&mut peer, "u").await;
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(*session.status.borrow(), Status::Ready);
    // A request the server never acknowledges ends the session at its deadline.
    session.handle.send(respond()).unwrap();
    recv(&mut peer).await;
    assert_eq!(
        failed(&mut session).await,
        Error::Timeout("response creation")
    );
}

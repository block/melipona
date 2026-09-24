use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

type Peer = WebSocketStream<TcpStream>;
async fn next(peer: &mut Peer) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(3), peer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            Message::Ping(_) | Message::Pong(_) => {}
            other => panic!("unexpected frame {other:?}"),
        }
    }
}
async fn put(peer: &mut Peer, event: Value) {
    peer.send(Message::Text(event.to_string().into()))
        .await
        .unwrap();
}
async fn open() -> (Session, Peer) {
    open_tools(ToolRegistry::empty()).await
}
async fn open_tools(tools: ToolRegistry) -> (Session, Peer) {
    open_config(tools, false).await
}
async fn open_config(tools: ToolRegistry, frankie: bool) -> (Session, Peer) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = Config::new(format!(
        "ws://{}/v1/realtime",
        listener.local_addr().unwrap()
    ));
    cfg.frankie_extensions = frankie;
    let connecting = tokio::spawn(Session::connect(cfg, tools));
    let (tcp, _) = listener.accept().await.unwrap();
    let mut peer = accept_async(tcp).await.unwrap();
    let mut session = connecting.await.unwrap().unwrap();
    assert_eq!(next(&mut peer).await["type"], "session.update");
    put(
        &mut peer,
        json!({"type":"session.updated","session":{"type":"realtime","frankie":{"playback_feedback":frankie}}}),
    )
    .await;
    while *session.status.borrow() != Status::Ready {
        session.status.changed().await.unwrap();
    }
    session.events.recv().await.unwrap();
    (session, peer)
}
fn spec() -> Scenario {
    serde_json::from_value(json!({"name":"synthetic","rounds":1,"settle_ms":32,"turn_timeout_ms":1000,
        "context":{"chars_per_turn":512},"turns":[{"name":"paragraph","text":"Explain a safe fictional picnic.","expect":{"min_words":5}}]})).unwrap()
}
fn evidence() -> (Evidence, PathBuf) {
    let path = std::env::temp_dir().join(format!("realtime-soak-{}.jsonl", uuid::Uuid::new_v4()));
    (
        Evidence {
            file: BufWriter::new(File::create(&path).unwrap()),
            start: Instant::now(),
            bytes: 0,
            output_dir: std::env::temp_dir(),
            audio_bytes: 0,
        },
        path,
    )
}
async fn reply(peer: &mut Peer, rid: &str, text: &str, usage: Value) {
    put(
        peer,
        json!({"type":"response.created","response":{"id":rid}}),
    )
    .await;
    put(peer,json!({"type":"response.output_text.delta","response_id":rid,"item_id":format!("i_{rid}"),"delta":text})).await;
    put(peer,json!({"type":"response.done","response":{"id":rid,"status":"completed","output":[],"usage":usage}})).await;
}

#[tokio::test]
async fn persistent_turns_ramp_context_and_preserve_unknown_usage() {
    let (mut session, mut peer) = open().await;
    let server = tokio::spawn(async move {
        for n in 0..3 {
            if n > 0 {
                let fill = next(&mut peer).await;
                assert!(
                    fill["item"]["content"][0]["text"]
                        .as_str()
                        .unwrap()
                        .contains("Reference data")
                );
            }
            assert_eq!(next(&mut peer).await["type"], "conversation.item.create");
            assert_eq!(next(&mut peer).await["type"], "response.create");
            reply(
                &mut peer,
                &format!("r{n}"),
                "A safe picnic includes clean water and shade.",
                if n == 0 {
                    Value::Null
                } else {
                    json!({"input_tokens":500*n,"input_token_details":{"cached_tokens":400*n}})
                },
            )
            .await;
        }
        // Keep the transport alive until the client finishes its settle clock.
        tokio::time::sleep(Duration::from_millis(300)).await;
    });
    let (_tx, mut receipts) = mpsc::channel(1);
    let (mut log, path) = evidence();
    let spec = spec();
    for n in 0..3 {
        let m = run_turn(
            &mut session,
            &mut receipts,
            &spec,
            &spec.turns[0],
            n,
            Path::new("."),
            &mut log,
        )
        .await
        .unwrap();
        assert_eq!(m.outcome, "pass");
        assert_eq!(m.context_chars_added, if n == 0 { 0 } else { 512 });
        assert_eq!(
            m.input_tokens,
            if n == 0 { None } else { Some(500 * n as u64) }
        );
        assert!(m.text_delivery_tokens_per_second.is_none());
    }
    session.finish().await.unwrap();
    server.await.unwrap();
    drop(log);
    fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn one_word_collapse_is_not_a_pass() {
    let (mut session, mut peer) = open().await;
    let server = tokio::spawn(async move {
        next(&mut peer).await;
        next(&mut peer).await;
        reply(&mut peer, "r", "Yes.", Value::Null).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let (_tx, mut receipts) = mpsc::channel(1);
    let (mut log, path) = evidence();
    let s = spec();
    let m = run_turn(
        &mut session,
        &mut receipts,
        &s,
        &s.turns[0],
        0,
        Path::new("."),
        &mut log,
    )
    .await
    .unwrap();
    assert_eq!(m.outcome, "fail");
    assert!(m.failures[0].contains("collapse"));
    session.finish().await.unwrap();
    server.await.unwrap();
    drop(log);
    fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn capacity_requires_exact_configured_code() {
    for (code, expected) in [("context_full", "capacity_refused"), ("rate_limit", "fail")] {
        let (mut session, mut peer) = open().await;
        let server = tokio::spawn(async move {
            next(&mut peer).await;
            next(&mut peer).await;
            put(&mut peer,json!({"type":"error","error":{"code":code,"message":"synthetic capacity wording is not sufficient"}})).await;
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        let (_tx, mut receipts) = mpsc::channel(1);
        let (mut log, path) = evidence();
        let mut s = spec();
        s.context.capacity_codes.push("context_full".into());
        let m = run_turn(
            &mut session,
            &mut receipts,
            &s,
            &s.turns[0],
            0,
            Path::new("."),
            &mut log,
        )
        .await
        .unwrap();
        assert_eq!(m.outcome, expected);
        session.finish().await.unwrap();
        server.await.unwrap();
        drop(log);
        fs::remove_file(path).unwrap();
    }
}

#[tokio::test]
async fn generation_done_does_not_end_buffered_playback() {
    let (mut session, mut peer) = open().await;
    let server = tokio::spawn(async move {
        next(&mut peer).await;
        next(&mut peer).await;
        put(
            &mut peer,
            json!({"type":"response.created","response":{"id":"r"}}),
        )
        .await;
        put(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"i","content_index":0,"delta":STANDARD.encode(vec![1;FRAME*2*10])})).await;
        put(
            &mut peer,
            json!({"type":"response.done","response":{"id":"r","status":"completed","output":[]}}),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(600)).await;
    });
    let (_tx, mut receipts) = mpsc::channel(1);
    let (mut log, path) = evidence();
    let mut s = spec();
    s.turns[0].expect.min_words = 0;
    let m = run_turn(
        &mut session,
        &mut receipts,
        &s,
        &s.turns[0],
        0,
        Path::new("."),
        &mut log,
    )
    .await
    .unwrap();
    assert_eq!(m.outcome, "pass");
    assert_eq!(m.rendered_audio_ms, 320);
    assert!(m.elapsed_ms >= 300.0);
    session.finish().await.unwrap();
    server.await.unwrap();
    drop(log);
    fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn real_echo_executor_records_exact_arguments() {
    let (tx, mut rx) = mpsc::channel(1);
    let echo = Echo {
        delay: Duration::ZERO,
        receipts: tx,
    };
    let result = echo
        .execute(
            ToolCall {
                call_id: "call1".into(),
                name: "echo".into(),
                arguments: json!({"text":"lighthouse"}),
            },
            ToolCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(result, json!({"text":"lighthouse"}));
    assert_eq!(rx.recv().await.unwrap()["call_id"], "call1");
}

#[test]
fn deterministic_fixture_and_redaction() {
    assert_eq!(scenario::filler(1, 2, 512), scenario::filler(1, 2, 512));
    assert_ne!(scenario::filler(1, 2, 512), scenario::filler(1, 3, 512));
    assert_eq!(scenario::filler(1, 2, 512).len(), 512);
    assert_eq!(
        scrub(json!({"session":{"client_secret":{"value":"secret"}}}))["session"]["client_secret"],
        "[redacted]"
    );
}

#[tokio::test]
async fn explicit_provider_telemetry_uses_reported_partitions_only() {
    for missing in [false, true] {
        let (mut session, mut peer) = open().await;
        let server = tokio::spawn(async move {
            next(&mut peer).await;
            next(&mut peer).await;
            put(
                &mut peer,
                json!({"type":"response.created","response":{"id":"r"}}),
            )
            .await;
            let mut stats = json!({"type":"example.metrics","response_id":"r","cached":400,"new":100,"speed":25.0,"memory":100000});
            if missing {
                stats.as_object_mut().unwrap().remove("new");
            }
            put(&mut peer, stats).await;
            put(&mut peer,json!({"type":"response.done","response":{"id":"r","status":"completed","output":[]}})).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let (_tx, mut receipts) = mpsc::channel(1);
        let (mut log, path) = evidence();
        let mut s = spec();
        s.turns[0].expect.min_words = 0;
        s.telemetry=Some(serde_json::from_value(json!({"event":"example.metrics","input_tokens":["/cached","/new"],"cached_input_tokens":"/cached","decode_tokens_per_second":"/speed","peak_memory_bytes":"/memory"})).unwrap());
        let m = run_turn(
            &mut session,
            &mut receipts,
            &s,
            &s.turns[0],
            0,
            Path::new("."),
            &mut log,
        )
        .await
        .unwrap();
        assert_eq!(m.input_tokens, if missing { None } else { Some(500) });
        assert_eq!(m.cached_input_tokens, Some(400));
        assert_eq!(m.reported_decode_tokens_per_second, Some(25.0));
        assert_eq!(m.reported_peak_memory_bytes, Some(100000));
        session.finish().await.unwrap();
        server.await.unwrap();
        drop(log);
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn code_free_capacity_refusal_requires_exact_opt_in_message() {
    let c = scenario::Context {
        capacity_messages: vec!["The configured context is full.".into()],
        ..Default::default()
    };
    assert!(
        c.refusal(&json!({"message":"The configured context is full."}))
            .is_some()
    );
    assert!(
        c.refusal(&json!({"message":"The configured context is full. Retry authentication."}))
            .is_none()
    );
}

#[tokio::test]
async fn speech_capture_survives_intermediate_cancelled_responses_and_pcm_end() {
    let (mut session, mut peer) = open().await;
    let fixture = std::env::temp_dir().join(format!("soak-overlap-{}.pcm", uuid::Uuid::new_v4()));
    fs::write(&fixture, vec![1; FRAME * 2 * 8]).unwrap();
    let server = tokio::spawn(async move {
        next(&mut peer).await;
        next(&mut peer).await;
        put(
            &mut peer,
            json!({"type":"response.created","response":{"id":"r1"}}),
        )
        .await;
        put(&mut peer,json!({"type":"response.output_text.delta","response_id":"r1","item_id":"i1","delta":"Abandoned draft words must not count."})).await;
        put(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r1","item_id":"i1","content_index":0,"delta":STANDARD.encode(vec![1;FRAME*2*20])})).await;
        let mut frames = 0;
        while frames < 8 {
            let event = next(&mut peer).await;
            if event["type"] == "conversation.item.truncate" {
                put(&mut peer,json!({"type":"conversation.item.truncated","item_id":event["item_id"],"content_index":0,"audio_end_ms":event["audio_end_ms"]})).await;
            }
            if event["type"] == "input_audio_buffer.append"
                && STANDARD
                    .decode(event["audio"].as_str().unwrap())
                    .unwrap()
                    .iter()
                    .any(|x| *x != 0)
            {
                frames += 1;
                if frames == 1 {
                    put(&mut peer,json!({"type":"input_audio_buffer.speech_started","item_id":"u1","audio_start_ms":0})).await;
                    put(&mut peer,json!({"type":"response.done","response":{"id":"r1","status":"cancelled","output":[]}})).await;
                    put(&mut peer,json!({"type":"input_audio_buffer.speech_stopped","item_id":"u1","audio_end_ms":32})).await;
                    put(
                        &mut peer,
                        json!({"type":"response.created","response":{"id":"r2"}}),
                    )
                    .await;
                    put(&mut peer,json!({"type":"input_audio_buffer.speech_started","item_id":"u2","audio_start_ms":32})).await;
                    put(&mut peer,json!({"type":"response.done","response":{"id":"r2","status":"cancelled","output":[]}})).await;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
        put(
            &mut peer,
            json!({"type":"input_audio_buffer.speech_stopped","item_id":"u2","audio_end_ms":256}),
        )
        .await;
        put(
            &mut peer,
            json!({"type":"input_audio_buffer.committed","item_id":"u2"}),
        )
        .await;
        reply(&mut peer, "r3", "The answer is six.", Value::Null).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
    });
    let (_tx, mut receipts) = mpsc::channel(1);
    let (mut log, path) = evidence();
    let mut s = spec();
    s.turns[0].expect.min_words = 0;
    s.turns[0].expect.contains = vec!["six".into()];
    s.turns[0].overlap = Some(scenario::Overlap {
        pcm: Some(fixture.to_string_lossy().into_owned()),
        after_played_ms: 32,
        interrupt: false,
        expect_clear: true,
    });
    let m = run_turn(
        &mut session,
        &mut receipts,
        &s,
        &s.turns[0],
        0,
        Path::new("."),
        &mut log,
    )
    .await
    .unwrap();
    assert_eq!(m.outcome, "pass", "{:?}", m.failures);
    assert_eq!(m.cancelled_responses, 2);
    assert_eq!(m.transcript, "The answer is six.");
    assert_eq!(m.output_words, 4);
    assert!(m.elapsed_ms > 400.0);
    session.finish().await.unwrap();
    server.await.unwrap();
    drop(log);
    fs::remove_file(path).unwrap();
    fs::remove_file(fixture).unwrap();
}

#[tokio::test]
async fn actual_tool_roundtrip_and_repeated_call_detection() {
    for call_count in [1, 2] {
        let (tx, mut receipts) = mpsc::channel(8);
        let tools=ToolRegistry::new(vec![Tool {name:"echo".into(),description:"Test".into(),
            parameters:json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false})}],
            Arc::new(Echo {delay:Duration::from_millis(10),receipts:tx})).unwrap();
        let (mut session, mut peer) = open_tools(tools).await;
        let server = tokio::spawn(async move {
            next(&mut peer).await;
            next(&mut peer).await;
            put(
                &mut peer,
                json!({"type":"response.created","response":{"id":"r1"}}),
            )
            .await;
            let mut calls = Vec::new();
            for i in 0..call_count {
                let item = json!({"id":format!("i{i}"),"type":"function_call","status":"completed","call_id":format!("call{i}"),"name":"echo","arguments":"{\"text\":\"lighthouse\"}"});
                put(
                    &mut peer,
                    json!({"type":"response.output_item.done","response_id":"r1","item":item}),
                )
                .await;
                calls.push(item);
            }
            for _ in 0..call_count {
                let result = next(&mut peer).await;
                assert_eq!(result["item"]["type"], "function_call_output");
                put(
                    &mut peer,
                    json!({"type":"conversation.item.created","item":result["item"]}),
                )
                .await;
            }
            put(&mut peer,json!({"type":"response.done","response":{"id":"r1","status":"completed","output":calls}})).await;
            assert_eq!(next(&mut peer).await["type"], "response.create");
            reply(
                &mut peer,
                "r2",
                "The echo returned lighthouse.",
                Value::Null,
            )
            .await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        let (mut log, path) = evidence();
        let mut s = spec();
        s.turns[0].expect.min_words = 0;
        s.turns[0].expect.tool_texts = vec!["lighthouse".into()];
        let m = run_turn(
            &mut session,
            &mut receipts,
            &s,
            &s.turns[0],
            0,
            Path::new("."),
            &mut log,
        )
        .await
        .unwrap();
        assert_eq!(m.tool_executions, call_count);
        assert_eq!(m.outcome, if call_count == 1 { "pass" } else { "fail" });
        session.finish().await.unwrap();
        server.await.unwrap();
        drop(log);
        fs::remove_file(path).unwrap();
    }
}

#[tokio::test]
async fn explicit_overlap_after_generation_done_uses_heard_samples() {
    let (mut session, mut peer) = open().await;
    let server = tokio::spawn(async move {
        next(&mut peer).await;
        next(&mut peer).await;
        put(
            &mut peer,
            json!({"type":"response.created","response":{"id":"r"}}),
        )
        .await;
        put(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"i","content_index":0,"delta":STANDARD.encode(vec![1;FRAME*2*20])})).await;
        put(
            &mut peer,
            json!({"type":"response.done","response":{"id":"r","status":"completed","output":[]}}),
        )
        .await;
        loop {
            let event = next(&mut peer).await;
            assert_ne!(
                event["type"], "response.cancel",
                "already-finished generation must not be cancelled"
            );
            if event["type"] == "conversation.item.truncate" {
                assert_eq!(event["audio_end_ms"], 96);
                put(&mut peer,json!({"type":"conversation.item.truncated","item_id":"i","content_index":0,"audio_end_ms":96})).await;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let (_tx, mut receipts) = mpsc::channel(1);
    let (mut log, path) = evidence();
    let mut s = spec();
    s.turns[0].expect.min_words = 0;
    s.turns[0].overlap = Some(scenario::Overlap {
        pcm: None,
        after_played_ms: 96,
        interrupt: true,
        expect_clear: true,
    });
    let m = run_turn(
        &mut session,
        &mut receipts,
        &s,
        &s.turns[0],
        0,
        Path::new("."),
        &mut log,
    )
    .await
    .unwrap();
    assert_eq!(m.outcome, "pass");
    assert_eq!(m.rendered_audio_ms, 96);
    assert!(m.time_to_stop_ms.unwrap() < 10.0);
    session.finish().await.unwrap();
    server.await.unwrap();
    drop(log);
    fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn speech_started_never_cancels_by_itself() {
    let (mut session, mut peer) = open().await;
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        next(&mut peer).await;
        next(&mut peer).await;
        put(
            &mut peer,
            json!({"type":"response.created","response":{"id":"r"}}),
        )
        .await;
        put(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"i","content_index":0,"delta":STANDARD.encode(vec![1;FRAME*2*8])})).await;
        put(
            &mut peer,
            json!({"type":"input_audio_buffer.speech_started","audio_start_ms":0,"item_id":"user"}),
        )
        .await;
        put(
            &mut peer,
            json!({"type":"input_audio_buffer.speech_stopped","audio_end_ms":32,"item_id":"user"}),
        )
        .await;
        put(
            &mut peer,
            json!({"type":"response.done","response":{"id":"r","status":"completed","output":[]}}),
        )
        .await;
        // Keep the peer alive for the whole playback/settle cycle. A fixed
        // sleep races the virtual player on slower machines.
        tokio::select! {
            _ = stopped => {},
            message = peer.next() => panic!("unexpected client write: {message:?}"),
        }
    });
    let (_tx, mut receipts) = mpsc::channel(1);
    let (mut log, path) = evidence();
    let mut s = spec();
    s.turns[0].expect.min_words = 0;
    let m = run_turn(
        &mut session,
        &mut receipts,
        &s,
        &s.turns[0],
        0,
        Path::new("."),
        &mut log,
    )
    .await
    .unwrap();
    assert_eq!(m.outcome, "pass");
    assert_eq!(m.rendered_audio_ms, 256);
    stop.send(()).unwrap();
    server.await.unwrap();
    session.finish().await.ok();
    drop(log);
    fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn manual_text_keeps_playback_feedback_without_filling_audio_buffer() {
    let (mut session, mut peer) = open_config(ToolRegistry::empty(), true).await;
    let server = tokio::spawn(async move {
        next(&mut peer).await;
        next(&mut peer).await;
        put(
            &mut peer,
            json!({"type":"response.created","response":{"id":"r"}}),
        )
        .await;
        put(&mut peer,json!({"type":"response.output_audio.delta","response_id":"r","item_id":"i","content_index":0,"delta":STANDARD.encode(vec![1;FRAME*2*10])})).await;
        put(
            &mut peer,
            json!({"type":"response.done","response":{"id":"r","status":"completed","output":[]}}),
        )
        .await;
        let end = Instant::now() + Duration::from_millis(300);
        let mut feedback = false;
        while Instant::now() < end {
            if let Ok(Some(Ok(Message::Text(text)))) =
                tokio::time::timeout(Duration::from_millis(10), peer.next()).await
            {
                let event: Value = serde_json::from_str(&text).unwrap();
                assert_ne!(event["type"], "input_audio_buffer.append");
                if event["type"] == "frankie.playback.position" {
                    feedback = true;
                }
            }
        }
        assert!(feedback);
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let (_tx, mut receipts) = mpsc::channel(1);
    let (mut log, path) = evidence();
    let mut s = spec();
    s.frankie = true;
    s.turns[0].expect.min_words = 0;
    let m = run_turn(
        &mut session,
        &mut receipts,
        &s,
        &s.turns[0],
        0,
        Path::new("."),
        &mut log,
    )
    .await
    .unwrap();
    assert_eq!(m.outcome, "pass");
    assert_eq!(m.rendered_audio_ms, 320);
    session.finish().await.unwrap();
    server.await.unwrap();
    drop(log);
    fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn budget_truncation_preserves_delivered_text_but_still_fails() {
    let (mut session, mut peer) = open().await;
    let server = tokio::spawn(async move {
        next(&mut peer).await;
        next(&mut peer).await;
        put(
            &mut peer,
            json!({"type":"response.created","response":{"id":"partial"}}),
        )
        .await;
        put(&mut peer, json!({"type":"response.output_text.delta","response_id":"partial","item_id":"item","delta":"A garden with silver flowers grows here"})).await;
        put(&mut peer, json!({"type":"response.done","response":{"id":"partial","status":"incomplete","status_details":{"reason":"max_output_tokens"},"output":[]}})).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let (_tx, mut receipts) = mpsc::channel(1);
    let (mut log, path) = evidence();
    let s = spec();
    let m = run_turn(
        &mut session,
        &mut receipts,
        &s,
        &s.turns[0],
        0,
        Path::new("."),
        &mut log,
    )
    .await
    .unwrap();
    assert_eq!(m.outcome, "fail");
    assert_eq!(m.output_words, 7);
    assert_eq!(m.transcript, "A garden with silver flowers grows here");
    assert_eq!(m.failures, ["response status: incomplete"]);
    session.finish().await.unwrap();
    server.await.unwrap();
    drop(log);
    fs::remove_file(path).unwrap();
}

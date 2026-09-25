use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

type Peer = WebSocketStream<TcpStream>;
async fn recv(peer: &mut Peer) -> Value {
    loop {
        if let Message::Text(text) = peer.next().await.unwrap().unwrap() {
            return serde_json::from_str(&text).unwrap();
        }
    }
}
async fn send(peer: &mut Peer, mut event: Value) {
    event["event_id"] = json!(uuid::Uuid::new_v4().to_string());
    peer.send(Message::Text(event.to_string().into()))
        .await
        .unwrap();
}

#[test]
fn unknown_checks_are_rejected_and_none_selects_all() {
    assert!(select(&["not_a_real_check".into()]).is_err());
    assert_eq!(select(&[]).unwrap(), CHECKS);
    assert_eq!(select(&["cancel".into()]).unwrap(), ["cancel"]);
}

#[test]
fn conformance_needs_every_check_to_pass() {
    let none = BTreeSet::new();
    let mut results = vec!["pass"; CHECKS.len()];
    assert_eq!(summary(&results, &none)["conformant"], true);
    results[0] = "skip";
    let skipped = summary(&results, &none);
    assert_eq!(skipped["conformant"], false);
    assert_eq!(skipped["skipped"], 1);
    assert_eq!(summary(&["pass"], &none)["conformant"], false);
    assert_eq!(summary(&[], &none)["conformant"], false);
}

/// The tool-result acknowledgement may arrive before or after the calling response ends.
async fn tool_flow(early_ack: bool) -> Outcome {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let mut peer = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        recv(&mut peer).await; // session.update
        send(
            &mut peer,
            json!({"type":"session.updated","session":{"type":"realtime"}}),
        )
        .await;
        recv(&mut peer).await; // user text
        recv(&mut peer).await; // response.create
        let response = |id, output| json!({"id":id,"status":"completed","conversation_id":"conv","output":output});
        send(
            &mut peer,
            json!({"type":"response.created","response":response("r1", json!([]))}),
        )
        .await;
        let call = json!({"id":"fc","type":"function_call","status":"completed","call_id":"c","name":"echo","arguments":r#"{"text":"lighthouse"}"#});
        send(&mut peer, json!({"type":"response.function_call_arguments.done","response_id":"r1","item_id":"fc","output_index":0,"call_id":"c","name":"echo","arguments":call["arguments"]})).await;
        send(&mut peer, json!({"type":"response.output_item.done","response_id":"r1","output_index":0,"item":call})).await;
        let result = recv(&mut peer).await;
        let ack = json!({"type":"conversation.item.added","item":result["item"]});
        if early_ack {
            send(&mut peer, ack.clone()).await;
        }
        send(
            &mut peer,
            json!({"type":"response.done","response":response("r1", json!([call]))}),
        )
        .await;
        if !early_ack {
            send(&mut peer, ack).await;
        }
        assert_eq!(recv(&mut peer).await["type"], "response.create");
        send(
            &mut peer,
            json!({"type":"response.created","response":response("r2", json!([]))}),
        )
        .await;
        send(&mut peer, json!({"type":"response.output_text.delta","response_id":"r2","item_id":"a","output_index":0,"content_index":0,"delta":"lighthouse"})).await;
        send(
            &mut peer,
            json!({"type":"response.done","response":response("r2", json!([]))}),
        )
        .await;
        // Close promptly so a missed event fails the check instead of waiting out its deadline.
        tokio::time::sleep(Duration::from_millis(500)).await;
    });
    run(&url, "tool_call").await.0
}

#[tokio::test]
async fn tool_result_acknowledgement_before_or_after_response_done_passes() {
    for early_ack in [false, true] {
        if let Err(Verdict::Fail(why) | Verdict::Skip(why)) = tool_flow(early_ack).await {
            panic!("early_ack={early_ack}: {why}");
        }
    }
}

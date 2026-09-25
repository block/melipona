#![cfg(all(feature = "mcp", unix))]

use melipona::{
    ToolCall, ToolCancellation,
    mcp::{Mcp, McpConfig, McpLimits},
};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};

fn config(servers: Value) -> McpConfig {
    serde_json::from_value(json!({"mcpServers":servers})).unwrap()
}
fn server(mode: &str, marker: Option<&PathBuf>) -> Value {
    let mut args = vec![
        "-u".to_string(),
        "-c".to_string(),
        include_str!("fixtures/mcp.py").to_string(),
        mode.into(),
    ];
    if let Some(marker) = marker {
        args.push(marker.to_string_lossy().into());
    }
    json!({"command":"python3", "args":args})
}
fn limits() -> McpLimits {
    McpLimits {
        startup_timeout: Duration::from_secs(3),
        shutdown_timeout: Duration::from_millis(150),
        ..Default::default()
    }
}
fn call(name: &str) -> ToolCall {
    ToolCall {
        call_id: "call".into(),
        name: name.into(),
        arguments: json!({"value":42}),
    }
}
async fn execute(mcp: &Mcp, name: &str) -> Value {
    tokio::time::timeout(
        Duration::from_secs(3),
        mcp.executor().execute(call(name), ToolCancellation::new()),
    )
    .await
    .unwrap()
    .unwrap()
}
struct Marker(PathBuf);
impl Marker {
    fn new() -> Self {
        Self(std::env::temp_dir().join(format!("melipona-mcp-{}", uuid::Uuid::new_v4())))
    }
    async fn wait(&self, needle: &str) -> String {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let text = std::fs::read_to_string(&self.0).unwrap_or_default();
                if text.contains(needle) {
                    return text;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
}
impl Drop for Marker {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn two_servers_route_same_name_preserve_metadata_and_both_result_forms() {
    let mcp = Mcp::connect(
        config(json!({"one":server("one",None),"two":server("two",None)})),
        limits(),
        4096,
    )
    .await
    .unwrap();
    assert_eq!(mcp.catalog().len(), 2);
    assert!(
        mcp.catalog()[0]
            .definition
            .annotations
            .as_ref()
            .unwrap()
            .destructive_hint
            .unwrap()
    );
    for name in ["one", "two"] {
        let result = execute(&mcp, &format!("{name}__echo")).await;
        assert_eq!(result["content"][0]["text"], name);
        assert_eq!(result["structuredContent"]["name"], "echo");
        assert_eq!(result["structuredContent"]["arguments"]["value"], 42);
        assert_eq!(result["isError"], false);
    }
    mcp.shutdown().await.unwrap();
}

#[tokio::test]
async fn application_protocol_errors_images_and_bounded_results() {
    for mode in ["application_error", "protocol_error", "image", "large"] {
        let mcp = Mcp::connect(config(json!({"dev":server(mode,None)})), limits(), 512)
            .await
            .unwrap();
        let result = execute(&mcp, "dev__echo").await;
        assert!(result.to_string().len() <= 512);
        match mode {
            "application_error" => {
                assert_eq!(result["isError"], true);
                assert_eq!(result["content"][0]["text"], mode);
            }
            "protocol_error" => {
                assert_eq!(result["error"], "mcp_protocol_error");
                assert_eq!(result["code"], -32603);
            }
            "image" => {
                assert_eq!(result["content"][1]["omitted"], true);
                assert!(result["content"][1].get("data").is_none());
            }
            "large" => assert_eq!(result["truncated"], true),
            _ => unreachable!(),
        }
        mcp.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn malformed_and_oversized_frames_fail_without_waiting_for_tool_deadline() {
    for mode in ["oversize", "malformed"] {
        let mcp = Mcp::connect(
            config(json!({"dev":server(mode,None)})),
            McpLimits {
                frame_bytes: 1024,
                ..limits()
            },
            4096,
        )
        .await
        .unwrap();
        assert_eq!(
            execute(&mcp, "dev__echo").await["error"],
            "mcp_transport_error"
        );
        mcp.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn invalid_selected_schema_missing_filter_cursor_cycle_and_startup_timeout_fail() {
    for mode in ["bad_schema", "cycle", "hang_init", "collision"] {
        let result = Mcp::connect(
            config(json!({"dev":server(mode,None)})),
            McpLimits {
                startup_timeout: if mode == "hang_init" {
                    Duration::from_millis(200)
                } else {
                    Duration::from_secs(3)
                },
                ..limits()
            },
            4096,
        )
        .await;
        assert!(result.is_err(), "{mode}");
        if mode == "bad_schema" {
            assert!(result.err().unwrap().to_string().contains("dev/echo"));
        }
    }
    let mut filtered = server("one", None);
    filtered["tools"] = json!(["missing"]);
    assert!(
        Mcp::connect(config(json!({"dev":filtered})), limits(), 4096)
            .await
            .is_err()
    );
    let mut excluded = server("bad_schema", None);
    excluded["tools"] = json!([]);
    let mcp = Mcp::connect(config(json!({"dev":excluded})), limits(), 4096)
        .await
        .unwrap();
    assert!(mcp.catalog().is_empty());
    mcp.shutdown().await.unwrap();
}

#[tokio::test]
async fn arbitrary_names_are_legal_aliases_and_route_to_original_names() {
    let mcp = Mcp::connect(config(json!({"dev":server("names",None)})), limits(), 4096)
        .await
        .unwrap();
    for tool in mcp.catalog() {
        assert!(tool.alias.len() <= 64);
        assert!(
            tool.alias
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        );
        assert_eq!(
            execute(&mcp, &tool.alias).await["structuredContent"]["name"],
            tool.definition.name.as_ref()
        );
    }
    mcp.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_pending_future_sends_cancellation_without_killing_server() {
    let marker = Marker::new();
    let mcp = Mcp::connect(
        config(json!({"dev":server("pending",Some(&marker.0))})),
        limits(),
        4096,
    )
    .await
    .unwrap();
    let executor = mcp.executor();
    let task = tokio::spawn(async move {
        executor
            .execute(call("dev__echo"), ToolCancellation::new())
            .await
    });
    marker.wait("called").await;
    task.abort();
    let _ = task.await;
    marker.wait("cancelled").await;
    mcp.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_before_admission_never_calls_server() {
    let marker = Marker::new();
    let mcp = Mcp::connect(
        config(json!({"dev":server("pending",Some(&marker.0))})),
        limits(),
        4096,
    )
    .await
    .unwrap();
    let cancel = ToolCancellation::new();
    cancel.cancel();
    let result = mcp
        .executor()
        .execute(call("dev__echo"), cancel)
        .await
        .unwrap();
    assert_eq!(result["outcome"], "not_dispatched");
    mcp.shutdown().await.unwrap();
    assert!(!marker.0.exists());
}

// Opt-in against the user's chosen real server build. Artifact directory is
// supplied by the runner, outside this repository; no private path is compiled in.
#[tokio::test]
#[ignore = "set MELIPONA_TEST_MCP_BINARY and MELIPONA_TEST_ARTIFACT_DIR"]
async fn real_dev_mcp_read_shell_and_cancel() {
    let binary = std::env::var("MELIPONA_TEST_MCP_BINARY").unwrap();
    let dir = PathBuf::from(std::env::var("MELIPONA_TEST_ARTIFACT_DIR").unwrap());
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("fixture.txt"), "MCP fixture sentinel\n").unwrap();
    let mcp = Mcp::connect(
        config(json!({"dev":{"command":binary,"cwd":dir,"tools":["read_file","shell"]}})),
        McpLimits::default(),
        65536,
    )
    .await
    .unwrap();
    let executor = mcp.executor();
    let read = executor
        .execute(
            ToolCall {
                call_id: "read".into(),
                name: "dev__read_file".into(),
                arguments: json!({"path":"fixture.txt"}),
            },
            ToolCancellation::new(),
        )
        .await
        .unwrap();
    assert!(read.to_string().contains("MCP fixture sentinel"));
    std::fs::write(dir.join("read-result.json"), read.to_string()).unwrap();
    let shell = executor
        .execute(
            ToolCall {
                call_id: "shell".into(),
                name: "dev__shell".into(),
                arguments: json!({"command":"printf 'MCP shell sentinel'"}),
            },
            ToolCancellation::new(),
        )
        .await
        .unwrap();
    assert!(shell.to_string().contains("MCP shell sentinel"));
    std::fs::write(dir.join("shell-result.json"), shell.to_string()).unwrap();
    let pid_file = dir.join("sleep.pid");
    let _ = std::fs::remove_file(&pid_file);
    let pending = tokio::spawn(async move {
        executor
            .execute(
                ToolCall {
                    call_id: "sleep".into(),
                    name: "dev__shell".into(),
                    arguments: json!({"command":"sleep 60 & echo $! > sleep.pid; wait"}),
                },
                ToolCancellation::new(),
            )
            .await
    });
    let pid = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(pid) = std::fs::read_to_string(&pid_file)
                && !pid.trim().is_empty()
            {
                break pid.trim().to_owned();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    // Deliberately shut down the owner before dropping/joining this live call.
    let shutdown = mcp.shutdown().await;
    let result = pending.await.unwrap().unwrap();
    assert_process_gone(&pid).await;
    shutdown.unwrap();
    assert_eq!(result["error"], "cancelled");
    std::fs::write(
        dir.join("cancel-result.txt"),
        "live future cancelled by adapter shutdown; shell sleep pid gone\n",
    )
    .unwrap();
}

async fn assert_process_gone(pid: &str) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let output = std::process::Command::new("ps")
                .args(["-p", pid, "-o", "stat="])
                .output()
                .unwrap();
            let state = String::from_utf8_lossy(&output.stdout);
            if !output.status.success() || state.trim().starts_with('Z') {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        // A failing test must not leave its own synthetic sleeper running.
        let _ = std::process::Command::new("kill")
            .args(["-KILL", pid])
            .status();
        panic!("owned test process {pid} survived shutdown");
    });
}

#[tokio::test]
async fn shutdown_and_drop_kill_uncooperative_same_group_descendants() {
    for explicit in [true, false] {
        let marker = Marker::new();
        let mcp = Mcp::connect(
            config(json!({"dev":server("descendant",Some(&marker.0))})),
            limits(),
            4096,
        )
        .await
        .unwrap();
        let executor = mcp.executor();
        let task = tokio::spawn(async move {
            executor
                .execute(call("dev__echo"), ToolCancellation::new())
                .await
        });
        let pid = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let text = std::fs::read_to_string(&marker.0).unwrap_or_default();
                if let Some(pid) = text.lines().find(|line| line.parse::<u32>().is_ok()) {
                    break pid.to_owned();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        let shutdown = if explicit {
            mcp.shutdown().await
        } else {
            drop(mcp);
            Ok(())
        };
        // Check (and clean up) the synthetic child even if shutdown reports failure.
        assert_process_gone(&pid).await;
        shutdown.unwrap();
    }
}

#[test]
fn cli_rejects_echo_with_mcp() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_melipona"))
        .env("REALTIME_URL", "ws://127.0.0.1:1")
        .env("REALTIME_MCP", r#"{"mcpServers":{}}"#)
        .arg("--echo-tool")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot be combined"));
}

#[test]
fn cli_strips_provider_environment_and_keeps_explicit_server_env() {
    let marker = Marker::new();
    let mut dev = server("environment", Some(&marker.0));
    dev["env"] = json!({"MCP_FIXTURE_VALUE":"configured"});
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_melipona"))
        .env("REALTIME_URL", "ws://127.0.0.1:1")
        .env("REALTIME_TOKEN", "synthetic-test-token-not-a-secret")
        .env(
            "REALTIME_MCP",
            json!({"mcpServers":{"dev":dev}}).to_string(),
        )
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert!(!output.status.success()); // Refused WebSocket, after MCP startup.
    let environment: Value =
        serde_json::from_str(&std::fs::read_to_string(&marker.0).unwrap()).unwrap();
    assert_eq!(
        environment,
        json!({"providerEnvPresent":false,"configured":"configured"})
    );
}

#[tokio::test]
async fn discovery_limits_fail_before_advertisement() {
    for bounds in [
        McpLimits {
            catalog_bytes: 1,
            ..limits()
        },
        McpLimits {
            tools_per_server: 1,
            ..limits()
        },
        McpLimits {
            pages_per_server: 1,
            ..limits()
        },
    ] {
        let mode = if bounds.pages_per_server == 1 {
            "cycle"
        } else {
            "names"
        };
        assert!(
            Mcp::connect(config(json!({"dev":server(mode,None)})), bounds, 4096)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn shutdown_cancels_live_calls_and_rejects_later_calls() {
    let marker = Marker::new();
    let mcp = Mcp::connect(
        config(json!({"dev":server("pending",Some(&marker.0))})),
        limits(),
        4096,
    )
    .await
    .unwrap();
    let executor = mcp.executor();
    let worker = executor.clone();
    let task = tokio::spawn(async move {
        worker
            .execute(call("dev__echo"), ToolCancellation::new())
            .await
    });
    marker.wait("called").await;
    mcp.shutdown().await.unwrap();
    assert_eq!(task.await.unwrap().unwrap()["error"], "cancelled");
    marker.wait("cancelled").await;
    assert_eq!(
        executor
            .execute(call("dev__echo"), ToolCancellation::new())
            .await
            .unwrap()["outcome"],
        "not_dispatched"
    );
}

// rmcp 1.8 has a 1024-slot peer queue. On this current-thread runtime no
// transport task runs between these polls, so the final send must await capacity.
#[tokio::test(flavor = "current_thread")]
async fn cancellation_while_sdk_admission_is_saturated_is_not_dispatched() {
    let mcp = Mcp::connect(
        config(json!({"dev":server("pending",None)})),
        McpLimits::default(),
        4096,
    )
    .await
    .unwrap();
    let executor = mcp.executor();
    let mut fillers = Vec::new();
    for _ in 0..1024 {
        let mut future = Box::pin(tokio::task::unconstrained(
            executor.execute(call("dev__echo"), ToolCancellation::new()),
        ));
        assert!(futures_util::poll!(&mut future).is_pending());
        fillers.push(future);
    }
    let cancel = ToolCancellation::new();
    let mut blocked = Box::pin(tokio::task::unconstrained(
        executor.execute(call("dev__echo"), cancel.clone()),
    ));
    assert!(futures_util::poll!(&mut blocked).is_pending());
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), blocked)
        .await
        .expect("cancellation must not wait for queue capacity or a tool response")
        .unwrap();
    assert_eq!(result["outcome"], "not_dispatched");
    drop(fillers);
    mcp.shutdown().await.unwrap();
}

#[tokio::test]
async fn exited_leader_does_not_leave_its_process_group_alive() {
    for explicit in [true, false] {
        let marker = Marker::new();
        let mode = if explicit {
            "exit_on_eof"
        } else {
            "exit_on_call"
        };
        let mcp = Mcp::connect(
            config(json!({"dev":server(mode,Some(&marker.0))})),
            limits(),
            4096,
        )
        .await
        .unwrap();
        let executor = mcp.executor();
        let task = tokio::spawn(async move {
            executor
                .execute(call("dev__echo"), ToolCancellation::new())
                .await
        });
        let text = marker.wait("leader=").await;
        let child = text
            .lines()
            .find(|line| line.parse::<u32>().is_ok())
            .unwrap();
        let leader = text
            .lines()
            .find_map(|line| line.strip_prefix("leader="))
            .unwrap();
        if !explicit {
            assert_process_gone(leader).await;
        }
        let result = if explicit {
            mcp.shutdown().await
        } else {
            drop(mcp);
            Ok(())
        };
        assert_process_gone(child).await;
        result.unwrap();
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn pasted_stdio_configs_and_exact_schema_error_identity() {
    let mut dev = server("one", None);
    dev["type"] = json!("stdio");
    let pasted: McpConfig =
        serde_json::from_value(json!({"mcpServers":{"dev":dev},"unrelated":true})).unwrap();
    Mcp::connect(pasted, limits(), 4096)
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    dev["type"] = json!("synthetic-private-value");
    let error = Mcp::connect(config(json!({"dev":dev})), limits(), 4096)
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("type must be stdio"));
    assert!(!error.contains("synthetic-private-value"));
    dev["unexpected"] = json!(true);
    assert!(serde_json::from_value::<McpConfig>(json!({"mcpServers":{"dev":dev}})).is_err());
    let error = Mcp::connect(
        config(json!({"dev":server("schema_prefix",None)})),
        limits(),
        4096,
    )
    .await
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains("(MCP dev/ab)"), "{error}");
    assert!(!error.contains("(MCP dev/a)"), "{error}");
}

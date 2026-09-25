use melipona::{
    Command, Config, Continuation, Session, Status, Tool, ToolCall, ToolCancellation, ToolExecutor,
    ToolFuture, ToolRegistry,
};
use serde_json::json;
use std::{
    io::{BufRead, Read, Write},
    sync::{Arc, mpsc},
    time::Duration,
};
use tokio::sync::watch;

struct Echo(Duration);
impl ToolExecutor for Echo {
    fn execute(&self, call: ToolCall, _: ToolCancellation) -> ToolFuture<'_> {
        Box::pin(async move {
            tokio::time::sleep(self.0).await;
            Ok(call.arguments)
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().any(|arg| arg == "--help") {
        println!(
            "melipona: JSONL stdin/stdout host\n\
Set REALTIME_URL (wss:// or loopback ws://), optional REALTIME_MODEL and REALTIME_TOKEN.\n\
Optional REALTIME_SESSION contains session JSON; REALTIME_TOOL_CONTINUATION=server\n\
leaves tool continuation to the endpoint. --echo-tool advertises one harmless echo tool.\n\
REALTIME_MCP supplies a mcpServers JSON object (requires the mcp build feature).\n\
Input: {{\"command\":\"text\",\"text\":\"Hello\"}}, {{\"command\":\"respond\"}}, {{\"command\":\"close\"}}.\n\
EOF closes the session. Stdout is JSONL events; this demo does not play audio."
        );
        return Ok(());
    }
    let mut config = Config::new(std::env::var("REALTIME_URL").map_err(|_| "set REALTIME_URL")?);
    config.model = std::env::var("REALTIME_MODEL").ok();
    config.frankie_extensions = std::env::var("REALTIME_FRANKIE_EXTENSIONS").as_deref() == Ok("1");
    config.bearer_token = std::env::var("REALTIME_TOKEN").ok();
    if let Ok(session) = std::env::var("REALTIME_SESSION") {
        config.session = serde_json::from_str(&session)?;
    }
    if let Ok(owner) = std::env::var("REALTIME_TOOL_CONTINUATION") {
        config.continuation = match owner.as_str() {
            "client" => Continuation::Client,
            "server" => Continuation::Server,
            _ => return Err("REALTIME_TOOL_CONTINUATION must be client or server".into()),
        };
    }
    let line_limit = config.limits.message_bytes;
    let echo_delay = std::env::var("REALTIME_ECHO_DELAY_MS")
        .unwrap_or_else(|_| "0".into())
        .parse::<u64>()
        .map_err(|_| "invalid REALTIME_ECHO_DELAY_MS")?;
    if echo_delay > 10_000 {
        return Err("REALTIME_ECHO_DELAY_MS exceeds 10000".into());
    }
    let echo = std::env::args().any(|arg| arg == "--echo-tool");
    let mcp_config = match std::env::var("REALTIME_MCP") {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => return Err("REALTIME_MCP must be UTF-8".into()),
    };
    if echo && mcp_config.is_some() {
        return Err("REALTIME_MCP cannot be combined with --echo-tool".into());
    }
    #[cfg(not(feature = "mcp"))]
    if mcp_config.is_some() {
        return Err("REALTIME_MCP requires building with --features mcp".into());
    }
    #[cfg(feature = "mcp")]
    let mcp = match mcp_config {
        Some(value) => {
            if value.len() > 1024 * 1024 {
                return Err("REALTIME_MCP exceeds 1 MiB".into());
            }
            let servers =
                serde_json::from_str(&value).map_err(|_| "invalid REALTIME_MCP configuration")?;
            Some(
                melipona::mcp::Mcp::connect(
                    servers,
                    Default::default(),
                    config.limits.tool_result_bytes,
                )
                .await?,
            )
        }
        None => None,
    };
    let tools = if echo {
        ToolRegistry::new(
            vec![Tool {
                name: "echo".into(),
                description: "Return the supplied text.".into(),
                parameters: json!({"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}),
            }],
            Arc::new(Echo(Duration::from_millis(echo_delay))),
        )?
    } else {
        ToolRegistry::empty()
    };
    #[cfg(feature = "mcp")]
    let tools = match &mcp {
        Some(mcp) => mcp.registry()?,
        None => tools,
    };
    let connected = Session::connect(config, tools).await;
    let mut session = match connected {
        Ok(session) => session,
        Err(error) => {
            #[cfg(feature = "mcp")]
            if let Some(mcp) = mcp {
                mcp.shutdown().await?;
            }
            return Err(error.into());
        }
    };
    let handle = session.handle.clone();
    let (io_status, mut io_errors) = watch::channel(None::<&'static str>);
    let input_errors = io_status.clone();
    // Dedicated OS threads isolate potentially blocking standard IO. They never own
    // the runtime/session; shutdown does not join a thread stuck in an external pipe.
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut input = stdin.lock();
        loop {
            let mut line = Vec::new();
            match (&mut input)
                .take((line_limit + 1) as u64)
                .read_until(b'\n', &mut line)
            {
                Ok(0) => {
                    let _ = handle.send(Command::Close);
                    break;
                }
                Ok(_) if line.len() > line_limit => {
                    input_errors.send_replace(Some("stdin line exceeds limit"));
                    break;
                }
                Ok(_) => {
                    let command = match serde_json::from_slice::<Command>(&line) {
                        Ok(command) => command,
                        Err(_) => {
                            input_errors.send_replace(Some("invalid command JSON"));
                            break;
                        }
                    };
                    let close = matches!(command, Command::Close);
                    if handle.send(command).is_err() {
                        input_errors.send_replace(Some("command queue full or closed"));
                        break;
                    }
                    if close {
                        break;
                    }
                }
                Err(_) => {
                    input_errors.send_replace(Some("stdin read failed"));
                    break;
                }
            }
        }
    });
    let (output, lines) = mpsc::sync_channel::<Vec<u8>>(64);
    let (written, mut output_done) = watch::channel(false);
    std::thread::spawn(move || {
        let stdout = std::io::stdout();
        let mut writer = stdout.lock();
        for line in lines {
            if writer
                .write_all(&line)
                .and_then(|_| writer.flush())
                .is_err()
            {
                io_status.send_replace(Some("stdout write failed"));
                break;
            }
        }
        written.send_replace(true);
    });
    let result: Result<(), Box<dyn std::error::Error>> = async {
        loop {
            tokio::select! {
                _=tokio::signal::ctrl_c()=>return Ok(()),
                changed=io_errors.changed()=>{
                    if changed.is_ok()
                        && let Some(error)=*io_errors.borrow() {return Err(error.into());}
                }
                event=session.events.recv()=>match event {
                    Some(event)=>{
                        let mut bytes=serde_json::to_vec(&event)?;
                        bytes.push(b'\n');
                        output.try_send(bytes).map_err(|_|"stdout queue full or closed")?;
                    }
                    None=>return Ok(()),
                },
                changed=session.status.changed()=>{
                    if changed.is_err() {return Ok(());}
                    match session.status.borrow().clone() {
                        Status::Failed(error)=>return Err(error.into()),
                        Status::Closed=>return Ok(()),
                        _=>{},
                    }
                }
            }
        }
    }
    .await;
    let finish = session.finish().await;
    #[cfg(feature = "mcp")]
    let mcp_finish = match mcp {
        Some(mcp) => mcp.shutdown().await,
        None => Ok(()),
    };
    drop(output);
    // Give a healthy pipe time to flush; never hang shutdown on a blocked consumer.
    let _ = tokio::time::timeout(Duration::from_millis(250), async {
        while !*output_done.borrow() {
            if output_done.changed().await.is_err() {
                break;
            }
        }
    })
    .await;
    result?;
    finish?;
    #[cfg(feature = "mcp")]
    mcp_finish?;
    Ok(())
}

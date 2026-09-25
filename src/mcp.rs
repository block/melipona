//! Optional local stdio MCP client. The host authorizes every exposed tool by
//! choosing its server configuration. This is not a sandbox or an approval UI.

use crate::{Error, Tool, ToolCall, ToolCancellation, ToolExecutor, ToolFuture, ToolRegistry};
use futures_util::{StreamExt, future::join_all};
use process_wrap::tokio::{ChildWrapper, CommandWrap, KillOnDrop};
use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, ClientRequest, PaginatedRequestParams, ServerResult},
    service::{Peer, PeerRequestOptions, RequestHandle, RunningService, ServiceError},
    transport::async_rw::JsonRpcMessageCodec,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio_util::{
    codec::{FramedRead, FramedWrite},
    task::{TaskTracker, task_tracker::TaskTrackerToken},
};

/// Trusted launch configuration, compatible with the local `mcpServers` envelope.
/// No interpolation, remote transport, or shell interpretation is performed.
#[derive(Deserialize)]
pub struct McpConfig {
    #[serde(rename = "mcpServers")]
    pub servers: BTreeMap<String, McpServer>,
}

/// A local executable. Omitted `tools` exposes all discovered tools without confirmation.
/// Environment values may contain secrets; deliberately not `Debug`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServer {
    /// Optional explicit transport. Only `stdio` is supported.
    #[serde(rename = "type")]
    pub transport: Option<String>,
    pub command: PathBuf,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    pub cwd: Option<PathBuf>,
    pub tools: Option<Vec<String>>,
}

/// Bounds on untrusted server data and startup/cleanup. Calls use the session's
/// tool deadline, not a second adapter timer.
#[derive(Clone)]
pub struct McpLimits {
    pub frame_bytes: usize,
    pub catalog_bytes: usize,
    pub tools_per_server: usize,
    pub pages_per_server: usize,
    pub servers: usize,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
}
impl Default for McpLimits {
    fn default() -> Self {
        Self {
            frame_bytes: 8 * 1024 * 1024,
            catalog_bytes: 8 * 1024 * 1024,
            tools_per_server: 1024,
            pages_per_server: 64,
            servers: 16,
            startup_timeout: Duration::from_secs(15),
            shutdown_timeout: Duration::from_secs(3),
        }
    }
}

/// Original metadata (including annotations) and the advertised model alias.
/// Annotations are server-supplied hints, not authorization.
pub struct McpTool {
    pub server: String,
    pub alias: String,
    pub definition: rmcp::model::Tool,
}
impl McpTool {
    fn tool(&self) -> Tool {
        Tool {
            name: self.alias.clone(),
            description: self
                .definition
                .description
                .as_deref()
                .or(self.definition.title.as_deref())
                .unwrap_or("")
                .to_owned(),
            parameters: Value::Object((*self.definition.input_schema).clone()),
        }
    }
}

struct Process(Box<dyn ChildWrapper>);
impl Drop for Process {
    fn drop(&mut self) {
        // Synchronous: also runs when there is no live Tokio runtime. On Unix
        // this kills the owned group even if its leader has already exited.
        let _ = self.0.start_kill();
    }
}
struct Server {
    service: RunningService<RoleClient, ()>,
    process: Process,
}

/// Owns servers independently of cloned executors. Prefer finishing sessions before
/// `shutdown`; shutdown also cancels live calls. Drop immediately kills owned groups.
pub struct Mcp {
    servers: Vec<Server>,
    catalog: Vec<McpTool>,
    executor: Arc<Executor>,
    limits: McpLimits,
}
struct Route {
    peer: Peer<RoleClient>,
    name: String,
}
struct Executor {
    routes: HashMap<String, Route>,
    cleanup: TaskTracker,
    cleanup_timeout: Duration,
    shutdown: ToolCancellation,
    result_bytes: usize,
}

impl Mcp {
    /// Launch, initialize and discover a fixed catalog. A startup failure drops
    /// all servers started by this call. `result_bytes` comes from `Config.limits`.
    pub async fn connect(
        config: McpConfig,
        limits: McpLimits,
        result_bytes: usize,
    ) -> Result<Self, Error> {
        if config.servers.len() > limits.servers
            || limits.frame_bytes == 0
            || limits.catalog_bytes == 0
            || limits.tools_per_server == 0
            || limits.pages_per_server == 0
            || limits.startup_timeout.is_zero()
            || limits.shutdown_timeout.is_zero()
            || result_bytes < 256
        {
            return Err(config_error("invalid MCP limits"));
        }
        let mut servers = Vec::new();
        let mut catalog = Vec::new();
        let mut routes = HashMap::new();
        for (name, config) in config.servers {
            if name.is_empty()
                || name.len() > 16
                || !name
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
            {
                return Err(config_error("MCP server names must match [a-z0-9-]{1,16}"));
            }
            if config
                .transport
                .as_deref()
                .is_some_and(|kind| kind != "stdio")
            {
                // Do not echo arbitrary config strings: even a misplaced value can be a secret.
                return Err(config_error(format!(
                    "MCP server {name}: unsupported transport; type must be stdio"
                )));
            }
            let result =
                tokio::time::timeout(limits.startup_timeout, connect_server(&config, &limits))
                    .await
                    .map_err(|_| config_error(format!("MCP server {name}: startup timed out")))?;
            let (server, tools) =
                result.map_err(|reason| config_error(format!("MCP server {name}: {reason}")))?;
            let peer = server.service.peer().clone();
            servers.push(server);
            let mut selected: HashSet<String> = config
                .tools
                .clone()
                .unwrap_or_default()
                .into_iter()
                .collect();
            let mut seen = HashSet::new();
            for definition in tools {
                if !seen.insert(definition.name.to_string()) {
                    return Err(config_error(format!(
                        "MCP server {name}: duplicate tool {}",
                        definition.name
                    )));
                }
                if config.tools.is_some() && !selected.remove(definition.name.as_ref()) {
                    continue;
                }
                if definition.execution.as_ref().and_then(|e| e.task_support)
                    == Some(rmcp::model::TaskSupport::Required)
                {
                    return Err(config_error(format!(
                        "MCP server {name}: tool {} requires unsupported task execution",
                        definition.name
                    )));
                }
                let alias = alias(&name, &definition.name);
                if routes.contains_key(&alias) {
                    return Err(config_error(format!(
                        "MCP alias collision for {name}/{}: {alias}",
                        definition.name
                    )));
                }
                routes.insert(
                    alias.clone(),
                    Route {
                        peer: peer.clone(),
                        name: definition.name.to_string(),
                    },
                );
                catalog.push(McpTool {
                    server: name.clone(),
                    alias,
                    definition,
                });
                if catalog.len() > crate::tools::MAX_TOOLS {
                    return Err(config_error(format!(
                        "too many exposed MCP tools (maximum {})",
                        crate::tools::MAX_TOOLS
                    )));
                }
            }
            if !selected.is_empty() {
                let mut missing: Vec<_> = selected.into_iter().collect();
                missing.sort();
                return Err(config_error(format!(
                    "MCP server {name}: selected tools not found: {}",
                    missing.join(", ")
                )));
            }
        }
        let executor = Arc::new(Executor {
            routes,
            cleanup: TaskTracker::new(),
            cleanup_timeout: limits.shutdown_timeout,
            shutdown: ToolCancellation::new(),
            result_bytes,
        });
        let owner = Self {
            servers,
            catalog,
            executor,
            limits,
        };
        // Reuse the core validator; never loosen schemas to fit the adapter.
        owner.registry()?;
        Ok(owner)
    }

    /// Static catalog for host policy wrappers, with original names and annotations.
    pub fn catalog(&self) -> &[McpTool] {
        &self.catalog
    }

    /// Function definitions with deterministic provider-safe aliases.
    pub fn tools(&self) -> Vec<Tool> {
        self.catalog.iter().map(McpTool::tool).collect()
    }

    /// Executor to use directly or wrap with application-specific authorization.
    pub fn executor(&self) -> Arc<dyn ToolExecutor> {
        self.executor.clone()
    }

    /// Build a registry using the same schema validation as native tools.
    pub fn registry(&self) -> Result<ToolRegistry, Error> {
        ToolRegistry::new(self.tools(), self.executor()).map_err(|e| {
            let mut detail = match e {
                Error::Config(detail) => detail,
                other => return other,
            };
            for tool in &self.catalog {
                if detail.strip_prefix("invalid schema for tool ") == Some(&tool.alias)
                    || detail.strip_prefix("tool definition exceeds size limit: ")
                        == Some(&tool.alias)
                {
                    detail.push_str(&format!(" (MCP {}/{})", tool.server, tool.definition.name));
                    break;
                }
            }
            config_error(detail)
        })
    }

    /// Cancel live calls, drain cancellation notifications, close services, allow
    /// cooperative exit, kill remaining owned groups, and reap their leaders.
    /// Descendants that leave the group are the server's responsibility, not a
    /// containment guarantee. A forced host kill cannot run this cleanup.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        let deadline = tokio::time::Instant::now() + self.limits.shutdown_timeout;
        // Reserve the final quarter for forced termination/reaping. All servers
        // share these deadlines, so N servers do not multiply the shutdown bound.
        let grace = deadline - self.limits.shutdown_timeout / 4;
        self.executor.shutdown.cancel();
        self.executor.cleanup.close();
        let _ = tokio::time::timeout_at(grace, self.executor.cleanup.wait()).await;
        let results = join_all(self.servers.iter_mut().map(|server| async move {
            let _ = tokio::time::timeout_at(grace, server.service.close()).await;
            let _ = tokio::time::timeout_at(grace, server.process.0.wait()).await;
            // Always kill the group: a clean leader exit does not mean its children exited.
            if let Err(error) = server.process.0.start_kill() {
                #[cfg(unix)]
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(config_error("MCP process group termination failed"));
                }
                #[cfg(not(unix))]
                if server.process.0.try_wait().ok().flatten().is_none() {
                    return Err(config_error("MCP process termination failed"));
                }
            }
            tokio::time::timeout_at(deadline, server.process.0.wait())
                .await
                .map_err(|_| config_error("MCP process reap timed out"))?
                .map_err(|_| config_error("MCP process reap failed"))?;
            Ok(())
        }))
        .await;
        results.into_iter().collect::<Result<Vec<_>, _>>()?;
        Ok(())
    }
}

fn config_error(message: impl Into<String>) -> Error {
    Error::Config(message.into())
}

async fn connect_server(
    config: &McpServer,
    limits: &McpLimits,
) -> Result<(Server, Vec<rmcp::model::Tool>), String> {
    let mut command = tokio::process::Command::new(&config.command);
    command
        .args(&config.args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // Preserve the usual executable environment, but not provider credentials or
    // other servers' configuration. Explicit env is trusted host configuration.
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("REALTIME_") {
            command.env_remove(key);
        }
    }
    command.envs(&config.env);
    if let Some(cwd) = &config.cwd {
        command.current_dir(cwd);
    }
    let mut command = CommandWrap::from(command);
    command.wrap(KillOnDrop);
    #[cfg(unix)]
    command.wrap(process_wrap::tokio::ProcessGroup::leader());
    #[cfg(windows)]
    command.wrap(process_wrap::tokio::JobObject);
    let mut process = Process(
        command
            .spawn()
            .map_err(|error| format!("could not launch local executable ({:?})", error.kind()))?,
    );
    let stdout = process.0.stdout().take().ok_or("missing stdout pipe")?;
    let stdin = process.0.stdin().take().ok_or("missing stdin pipe")?;
    // FramedRead yields None after a decode error, but can resume if polled
    // again. Stop permanently on that error, independently of the consumer.
    let stream = FramedRead::new(
        stdout,
        JsonRpcMessageCodec::<rmcp::service::RxJsonRpcMessage<RoleClient>>::new_with_max_length(
            limits.frame_bytes,
        ),
    )
    .scan((), |_, message| futures_util::future::ready(message.ok()));
    let sink = FramedWrite::new(
        stdin,
        JsonRpcMessageCodec::<rmcp::service::TxJsonRpcMessage<RoleClient>>::default(),
    );
    let service = ().serve((sink, stream)).await.map_err(|_| "MCP initialization failed")?;
    let server = Server { service, process };
    let mut tools = Vec::new();
    let mut cursor = None;
    let mut cursors = HashSet::new();
    let mut bytes = 0usize;
    for _ in 0..limits.pages_per_server {
        let page = server
            .service
            .list_tools(
                cursor.map(|cursor| PaginatedRequestParams::default().with_cursor(Some(cursor))),
            )
            .await
            .map_err(|_| "tool discovery failed")?;
        bytes = bytes.saturating_add(
            serde_json::to_vec(&page)
                .map_err(|_| "invalid catalog")?
                .len(),
        );
        if bytes > limits.catalog_bytes
            || tools.len().saturating_add(page.tools.len()) > limits.tools_per_server
        {
            return Err("tool discovery exceeds limits".into());
        }
        tools.extend(page.tools);
        match page.next_cursor {
            None => return Ok((server, tools)),
            Some(next) if cursors.insert(next.clone()) => cursor = Some(next),
            Some(_) => return Err("tool discovery cursor cycle".into()),
        }
    }
    Err("tool discovery page limit exceeded".into())
}

fn alias(server: &str, tool: &str) -> String {
    let plain = format!("{server}__{tool}");
    if plain.len() <= 64
        && plain
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
    {
        return plain;
    }
    let hash = Sha256::digest(format!("{server}\0{tool}").as_bytes());
    let prefix: String = plain
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(47)
        .collect();
    let suffix = format!("{hash:x}");
    format!("{prefix}_{}", &suffix[..16])
}

struct Pending {
    // Fields drop after Drop::drop, so cancellation is tracked before this releases.
    _active: TaskTrackerToken,
    handle: Option<RequestHandle<RoleClient>>,
    cleanup: TaskTracker,
    timeout: Duration,
}
impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take()
            && tokio::runtime::Handle::try_current().is_ok()
        {
            let timeout = self.timeout;
            self.cleanup.spawn(async move {
                let _ = tokio::time::timeout(
                    timeout,
                    handle.cancel(Some(
                        "host cancelled tool; effects may already have occurred".into(),
                    )),
                )
                .await;
            });
        }
    }
}
impl ToolExecutor for Executor {
    fn execute(&self, call: ToolCall, cancel: ToolCancellation) -> ToolFuture<'_> {
        Box::pin(async move {
            // Track the execution itself, not only the later cancel task: shutdown
            // must not observe an empty tracker before a woken Pending guard drops.
            let active = self.cleanup.token();
            let route = self.routes.get(&call.name).ok_or("unknown MCP tool")?;
            let arguments = call
                .arguments
                .as_object()
                .ok_or("MCP tool arguments must be an object")?
                .clone();
            let request = ClientRequest::CallToolRequest(rmcp::model::CallToolRequest::new(
                CallToolRequestParams::new(route.name.clone()).with_arguments(arguments),
            ));
            let handle = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(json!({"error":"cancelled","outcome":"not_dispatched"})),
                _ = self.shutdown.cancelled() => return Ok(json!({"error":"cancelled","outcome":"not_dispatched"})),
                handle = route.peer.send_cancellable_request(request, PeerRequestOptions::no_options()) => handle,
            };
            let handle = match handle {
                Ok(handle) => handle,
                Err(error) => return Ok(service_error(error)),
            };
            // No await between admission and guard ownership. rmcp 1.8's send has
            // no suspension after queue admission when request options are empty.
            let mut pending = Pending {
                _active: active,
                handle: Some(handle),
                cleanup: self.cleanup.clone(),
                timeout: self.cleanup_timeout,
            };
            let response = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(json!({"error":"cancelled","outcome":"unknown"})),
                _ = self.shutdown.cancelled() => return Ok(json!({"error":"cancelled","outcome":"unknown"})),
                response = &mut pending.handle.as_mut().ok_or("missing MCP request")?.rx => response,
            };
            pending.handle.take();
            let value = match response.unwrap_or(Err(ServiceError::TransportClosed)) {
                Ok(ServerResult::CallToolResult(result)) => map_result(result),
                Ok(_) => json!({"error":"unexpected_mcp_result","outcome":"unknown"}),
                Err(error) => service_error(error),
            };
            Ok(bound_result(value, self.result_bytes))
        })
    }
}
fn service_error(error: ServiceError) -> Value {
    match error {
        ServiceError::McpError(error) => {
            json!({"error":"mcp_protocol_error","code":error.code,"message":error.message,"data":error.data})
        }
        _ => json!({"error":"mcp_transport_error","outcome":"unknown"}),
    }
}
fn map_result(result: rmcp::model::CallToolResult) -> Value {
    let content: Vec<Value> = result
        .content
        .into_iter()
        .map(|content| {
            let mut value = serde_json::to_value(content).unwrap_or(Value::Null);
            if value["type"] != "text" {
                if let Some(object) = value.as_object_mut() {
                    object.remove("data");
                    object.insert("omitted".into(), json!(true));
                }
                if let Some(resource) = value.get_mut("resource").and_then(Value::as_object_mut) {
                    resource.remove("blob");
                }
            }
            value
        })
        .collect();
    json!({"content":content,"structuredContent":result.structured_content,"isError":result.is_error.unwrap_or(false)})
}
fn bound_result(value: Value, limit: usize) -> Value {
    let encoded = value.to_string();
    if encoded.len() <= limit {
        return value;
    }
    // Prefer readable tool text over a doubly encoded JSON fragment. For
    // structured-only results retain a labelled JSON preview instead.
    let text = value["content"]
        .as_array()
        .map(|content| {
            content
                .iter()
                .filter_map(|item| item.get("text").and_then(Value::as_str))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .unwrap_or_default();
    let (source, preview) = if text.is_empty() {
        ("json", encoded.as_str())
    } else {
        ("text", text.as_str())
    };
    let mut budget = limit / 2;
    loop {
        let mut head = budget.min(preview.len());
        while !preview.is_char_boundary(head) {
            head -= 1;
        }
        let mut tail = preview.len().saturating_sub(budget).max(head);
        while !preview.is_char_boundary(tail) {
            tail += 1;
        }
        let result = json!({"truncated":true,"isError":value["isError"].as_bool().unwrap_or(value.get("error").is_some()),
            "originalBytes":encoded.len(),"previewSource":source,"head":&preview[..head],"tail":&preview[tail..]});
        if result.to_string().len() <= limit {
            return result;
        }
        budget /= 2;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_are_order_independent_and_truncation_is_utf8_and_json_safe() {
        let names = [
            "plain",
            "has.dot",
            "has__separator",
            "長い名前",
            &"long".repeat(40),
        ];
        let forward: BTreeMap<_, _> = names
            .iter()
            .map(|name| (*name, alias("dev", name)))
            .collect();
        let reverse: BTreeMap<_, _> = names
            .iter()
            .rev()
            .map(|name| (*name, alias("dev", name)))
            .collect();
        assert_eq!(forward, reverse);
        assert_eq!(forward.values().collect::<HashSet<_>>().len(), names.len());
        let text = "readable line\nwith \"quotes\" and Unicode 界 ".repeat(4000);
        let result = bound_result(
            json!({"content":[{"type":"text","text":text}],"isError":true}),
            65536,
        );
        assert_eq!(result["previewSource"], "text");
        assert!(
            result["head"]
                .as_str()
                .unwrap()
                .contains("line\nwith \"quotes\"")
        );
        assert!(result.to_string().len() > 32768);
        assert!(result.to_string().len() <= 65536);
        for size in [256, 257, 512, 1024] {
            let result = bound_result(
                json!({"isError":true,"structuredContent":{"value":"界\\\"\n".repeat(2048)}}),
                size,
            );
            assert_eq!(result["truncated"], true);
            assert_eq!(result["isError"], true);
            assert!(result.to_string().len() <= size);
        }
    }
}

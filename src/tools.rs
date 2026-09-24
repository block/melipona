use crate::Error;
use serde_json::{Value, json};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};
use tokio_util::sync::CancellationToken;

pub type ToolFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, String>> + Send + 'a>>;

#[derive(Clone, Debug)]
pub struct ToolCall {
    pub call_id: String,
    pub name: String,
    pub arguments: Value,
}

/// Trusted host code. Never block a Tokio worker; use spawn_blocking for blocking work.
/// Cancellation drops this future and signals the token; it cannot undo an external write.
pub trait ToolExecutor: Send + Sync + 'static {
    fn execute(&self, call: ToolCall, cancel: CancellationToken) -> ToolFuture<'_>;
}

pub struct Tool {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

struct InlineSchemas;
impl jsonschema::Retrieve for InlineSchemas {
    fn retrieve(
        &self,
        _: &jsonschema::Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("external schema retrieval is disabled".into())
    }
}

pub struct ToolRegistry {
    pub(crate) executor: Arc<dyn ToolExecutor>,
    pub(crate) validators: HashMap<String, jsonschema::Validator>,
    pub(crate) definitions: Vec<Value>,
}

impl ToolRegistry {
    pub fn new(tools: Vec<Tool>, executor: Arc<dyn ToolExecutor>) -> Result<Self, Error> {
        if tools.len() > 128 {
            return Err(Error::Config("too many tool schemas (maximum 128)".into()));
        }
        let mut validators = HashMap::new();
        let mut definitions = Vec::new();
        for tool in tools {
            if tool.parameters.to_string().len() > 256 * 1024
                || tool.description.len() > 16 * 1024
                || tool.name.len() > 64
            {
                return Err(Error::Config("tool definition exceeds size limit".into()));
            }
            if tool.name.is_empty() || validators.contains_key(&tool.name) {
                return Err(Error::Config("empty or duplicate tool name".into()));
            }
            let validator = jsonschema::options()
                .with_retriever(InlineSchemas)
                .build(&tool.parameters)
                .map_err(|_| Error::Config(format!("invalid schema for tool {}", tool.name)))?;
            validators.insert(tool.name.clone(), validator);
            definitions.push(json!({"type":"function","name":tool.name,
                "description":tool.description,"parameters":tool.parameters}));
        }
        Ok(Self {
            executor,
            validators,
            definitions,
        })
    }

    pub fn empty() -> Self {
        struct None;
        impl ToolExecutor for None {
            fn execute(&self, _: ToolCall, _: CancellationToken) -> ToolFuture<'_> {
                Box::pin(async { Err("unknown_tool".into()) })
            }
        }
        Self {
            executor: Arc::new(None),
            validators: HashMap::new(),
            definitions: Vec::new(),
        }
    }
}

//! Required top-level fields of every GA server event, generated from
//! <https://developers.openai.com/api/reference/resources/realtime/server-events.md>
//! (retrieved 2026-09-24): each non-optional field, as `name:kind` where kind is
//! s(tring), n(umber), o(bject) or a(rray). Regenerate when the reference changes.
pub const REQUIRED: &[(&str, &str)] = &[
    ("error", "error:o event_id:s"),
    ("session.created", "event_id:s session:o"),
    ("session.updated", "event_id:s session:o"),
    ("conversation.item.added", "event_id:s item:o"),
    ("conversation.item.done", "event_id:s item:o"),
    ("conversation.item.retrieved", "event_id:s item:o"),
    (
        "conversation.item.input_audio_transcription.completed",
        "content_index:n event_id:s item_id:s transcript:s usage:o",
    ),
    (
        "conversation.item.input_audio_transcription.delta",
        "event_id:s item_id:s",
    ),
    (
        "conversation.item.input_audio_transcription.segment",
        "id:s content_index:n end:n event_id:s item_id:s speaker:s start:n text:s",
    ),
    (
        "conversation.item.input_audio_transcription.failed",
        "content_index:n error:o event_id:s item_id:s",
    ),
    (
        "conversation.item.truncated",
        "audio_end_ms:n content_index:n event_id:s item_id:s",
    ),
    ("conversation.item.deleted", "event_id:s item_id:s"),
    ("input_audio_buffer.committed", "event_id:s item_id:s"),
    (
        "input_audio_buffer.dtmf_event_received",
        "event:s received_at:n",
    ),
    ("input_audio_buffer.cleared", "event_id:s"),
    (
        "input_audio_buffer.speech_started",
        "audio_start_ms:n event_id:s item_id:s",
    ),
    (
        "input_audio_buffer.speech_stopped",
        "audio_end_ms:n event_id:s item_id:s",
    ),
    (
        "input_audio_buffer.timeout_triggered",
        "audio_end_ms:n audio_start_ms:n event_id:s item_id:s",
    ),
    ("output_audio_buffer.started", "event_id:s response_id:s"),
    ("output_audio_buffer.stopped", "event_id:s response_id:s"),
    ("output_audio_buffer.cleared", "event_id:s response_id:s"),
    ("response.created", "event_id:s response:o"),
    ("response.done", "event_id:s response:o"),
    (
        "response.output_item.added",
        "event_id:s item:o output_index:n response_id:s",
    ),
    (
        "response.output_item.done",
        "event_id:s item:o output_index:n response_id:s",
    ),
    (
        "response.content_part.added",
        "content_index:n event_id:s item_id:s output_index:n part:o response_id:s",
    ),
    (
        "response.content_part.done",
        "content_index:n event_id:s item_id:s output_index:n part:o response_id:s",
    ),
    (
        "response.output_text.delta",
        "content_index:n delta:s event_id:s item_id:s output_index:n response_id:s",
    ),
    (
        "response.output_text.done",
        "content_index:n event_id:s item_id:s output_index:n response_id:s text:s",
    ),
    (
        "response.output_audio_transcript.delta",
        "content_index:n delta:s event_id:s item_id:s output_index:n response_id:s",
    ),
    (
        "response.output_audio_transcript.done",
        "content_index:n event_id:s item_id:s output_index:n response_id:s transcript:s",
    ),
    (
        "response.output_audio.delta",
        "content_index:n delta:s event_id:s item_id:s output_index:n response_id:s",
    ),
    (
        "response.output_audio.done",
        "content_index:n event_id:s item_id:s output_index:n response_id:s",
    ),
    (
        "response.function_call_arguments.delta",
        "call_id:s delta:s event_id:s item_id:s output_index:n response_id:s",
    ),
    (
        "response.function_call_arguments.done",
        "arguments:s call_id:s event_id:s item_id:s name:s output_index:n response_id:s",
    ),
    (
        "response.mcp_call_arguments.delta",
        "delta:s event_id:s item_id:s output_index:n response_id:s",
    ),
    (
        "response.mcp_call_arguments.done",
        "arguments:s event_id:s item_id:s output_index:n response_id:s",
    ),
    (
        "response.mcp_call.in_progress",
        "event_id:s item_id:s output_index:n",
    ),
    (
        "response.mcp_call.completed",
        "event_id:s item_id:s output_index:n",
    ),
    (
        "response.mcp_call.failed",
        "event_id:s item_id:s output_index:n",
    ),
    ("mcp_list_tools.in_progress", "event_id:s item_id:s"),
    ("mcp_list_tools.completed", "event_id:s item_id:s"),
    ("mcp_list_tools.failed", "event_id:s item_id:s"),
    ("rate_limits.updated", "event_id:s rate_limits:a"),
    ("conversation.created", "conversation:o event_id:s"),
    ("conversation.item.created", "event_id:s item:o"),
];

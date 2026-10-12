use serde_json::{Map, Value};

/// Event names on the Responses stream, shared by the SSE and WebSocket transports. Only names
/// LiteLLM branches on are variants; everything else rides in `Other` untouched.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    strum::AsRefStr,
    strum::EnumString,
    strum::Display,
    serde_with::DeserializeFromStr,
    serde_with::SerializeDisplay,
)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(from = "String", into = "String"))]
pub enum ResponsesEventType {
    #[strum(serialize = "response.create")]
    ResponseCreate,
    #[strum(serialize = "response.created")]
    ResponseCreated,
    #[strum(serialize = "response.in_progress")]
    ResponseInProgress,
    #[strum(serialize = "response.completed")]
    ResponseCompleted,
    #[strum(serialize = "response.failed")]
    ResponseFailed,
    #[strum(serialize = "response.incomplete")]
    ResponseIncomplete,
    #[strum(serialize = "error")]
    Error,
    #[strum(serialize = "response.output_item.added")]
    OutputItemAdded,
    #[strum(serialize = "response.output_item.done")]
    OutputItemDone,
    #[strum(serialize = "response.content_part.added")]
    ContentPartAdded,
    #[strum(serialize = "response.content_part.done")]
    ContentPartDone,
    #[strum(serialize = "response.output_text.delta")]
    OutputTextDelta,
    #[strum(serialize = "response.output_text.annotation.added")]
    OutputTextAnnotationAdded,
    #[strum(serialize = "response.output_text.done")]
    OutputTextDone,
    #[strum(serialize = "response.refusal.delta")]
    RefusalDelta,
    #[strum(serialize = "response.refusal.done")]
    RefusalDone,
    #[strum(serialize = "response.function_call_arguments.delta")]
    FunctionCallArgumentsDelta,
    #[strum(serialize = "response.function_call_arguments.done")]
    FunctionCallArgumentsDone,
    #[strum(serialize = "response.reasoning_summary_part.added")]
    ReasoningSummaryPartAdded,
    #[strum(serialize = "response.reasoning_summary_part.done")]
    ReasoningSummaryPartDone,
    #[strum(serialize = "response.reasoning_summary_text.delta")]
    ReasoningSummaryTextDelta,
    #[strum(serialize = "response.reasoning_summary_text.done")]
    ReasoningSummaryTextDone,
    #[strum(default, transparent)]
    Other(String),
}

impl ResponsesEventType {
    /// The last event of a response. `response.completed`, `response.incomplete` and
    /// `response.failed` each close the response and carry its final body; `error` aborts it.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::ResponseCompleted | Self::ResponseFailed | Self::ResponseIncomplete | Self::Error
        )
    }

    /// A terminal event after which the response is not a success.
    pub fn is_failure(&self) -> bool {
        matches!(
            self,
            Self::ResponseFailed | Self::ResponseIncomplete | Self::Error
        )
    }
}

#[macro_rules_attribute::apply(crate::wire_type)]
pub struct ResponsesStreamEvent {
    #[serde(rename = "type")]
    pub event_type: ResponsesEventType,
    #[serde(flatten)]
    pub data: Map<String, Value>,
}

impl ResponsesStreamEvent {
    pub fn model(&self) -> Option<&str> {
        let model = self.data.get("model").and_then(Value::as_str);
        if model.is_some() {
            return model;
        }
        self.data
            .get("response")
            .and_then(Value::as_object)
            .and_then(|response| response.get("model"))
            .and_then(Value::as_str)
    }

    pub fn is_response_create(&self) -> bool {
        self.event_type == ResponsesEventType::ResponseCreate
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;

    #[rstest]
    #[case::create("response.create", ResponsesEventType::ResponseCreate)]
    #[case::created("response.created", ResponsesEventType::ResponseCreated)]
    #[case::in_progress("response.in_progress", ResponsesEventType::ResponseInProgress)]
    #[case::completed("response.completed", ResponsesEventType::ResponseCompleted)]
    #[case::failed("response.failed", ResponsesEventType::ResponseFailed)]
    #[case::incomplete("response.incomplete", ResponsesEventType::ResponseIncomplete)]
    #[case::error("error", ResponsesEventType::Error)]
    #[case::item_added("response.output_item.added", ResponsesEventType::OutputItemAdded)]
    #[case::item_done("response.output_item.done", ResponsesEventType::OutputItemDone)]
    #[case::part_added("response.content_part.added", ResponsesEventType::ContentPartAdded)]
    #[case::part_done("response.content_part.done", ResponsesEventType::ContentPartDone)]
    #[case::text("response.output_text.delta", ResponsesEventType::OutputTextDelta)]
    #[case::annotation(
        "response.output_text.annotation.added",
        ResponsesEventType::OutputTextAnnotationAdded
    )]
    #[case::text_done("response.output_text.done", ResponsesEventType::OutputTextDone)]
    #[case::refusal("response.refusal.delta", ResponsesEventType::RefusalDelta)]
    #[case::refusal_done("response.refusal.done", ResponsesEventType::RefusalDone)]
    #[case::tool(
        "response.function_call_arguments.delta",
        ResponsesEventType::FunctionCallArgumentsDelta
    )]
    #[case::tool_done(
        "response.function_call_arguments.done",
        ResponsesEventType::FunctionCallArgumentsDone
    )]
    #[case::summary_part(
        "response.reasoning_summary_part.added",
        ResponsesEventType::ReasoningSummaryPartAdded
    )]
    #[case::summary_part_done(
        "response.reasoning_summary_part.done",
        ResponsesEventType::ReasoningSummaryPartDone
    )]
    #[case::summary_text(
        "response.reasoning_summary_text.delta",
        ResponsesEventType::ReasoningSummaryTextDelta
    )]
    #[case::summary_text_done(
        "response.reasoning_summary_text.done",
        ResponsesEventType::ReasoningSummaryTextDone
    )]
    #[case::unknown("future.event", ResponsesEventType::Other("future.event".into()))]
    #[case::empty("", ResponsesEventType::Other(String::new()))]
    #[case::case_sensitive(
        "Response.Completed",
        ResponsesEventType::Other("Response.Completed".into())
    )]
    #[case::escaped("future\"\\\n", ResponsesEventType::Other("future\"\\\n".into()))]
    fn event_types_round_trip(#[case] wire: &str, #[case] expected: ResponsesEventType) {
        assert_eq!(
            serde_json::from_value::<ResponsesEventType>(json!(wire)).unwrap(),
            expected
        );
        assert_eq!(serde_json::to_value(&expected).unwrap(), json!(wire));
        assert_eq!(expected.as_ref(), wire);
        assert_eq!(expected.to_string(), wire);
    }

    /// Python's `ResponsesAPIStreamingIterator` stores the response on `response.completed`,
    /// `response.incomplete` and `response.failed`, and raises on `response.failed` and `error`.
    #[rstest]
    #[case::completed(ResponsesEventType::ResponseCompleted, true, false)]
    #[case::incomplete(ResponsesEventType::ResponseIncomplete, true, true)]
    #[case::failed(ResponsesEventType::ResponseFailed, true, true)]
    #[case::error(ResponsesEventType::Error, true, true)]
    #[case::create(ResponsesEventType::ResponseCreate, false, false)]
    #[case::created(ResponsesEventType::ResponseCreated, false, false)]
    #[case::in_progress(ResponsesEventType::ResponseInProgress, false, false)]
    #[case::delta(ResponsesEventType::OutputTextDelta, false, false)]
    #[case::item_done(ResponsesEventType::OutputItemDone, false, false)]
    #[case::unknown(ResponsesEventType::Other("response.future".into()), false, false)]
    #[case::unknown_failed(ResponsesEventType::Other("response.mcp_call.failed".into()), false, false)]
    fn terminal_and_failure_events_match_the_python_iterator(
        #[case] event_type: ResponsesEventType,
        #[case] terminal: bool,
        #[case] failure: bool,
    ) {
        assert_eq!(event_type.is_terminal(), terminal);
        assert_eq!(event_type.is_failure(), failure);
    }

    #[rstest]
    #[case::number("17")]
    #[case::boolean("true")]
    #[case::null("null")]
    #[case::array("[]")]
    #[case::object("{}")]
    fn event_types_reject_non_strings(#[case] wire: &str) {
        assert!(serde_json::from_str::<ResponsesEventType>(wire).is_err());
    }

    #[rstest]
    #[case::text(json!({"type":"response.output_text.delta","delta":"hello","sequence_number":3,"future":null}))]
    #[case::tool(json!({"type":"response.function_call_arguments.delta","delta":"{\"x\":","item_id":"fc_1","output_index":0}))]
    #[case::unknown(json!({"type":"future.event","payload":{"items":[1,null]},"future":true}))]
    #[case::terminal(json!({"type":"response.completed","response":{"id":"r","output":[],"future":null}}))]
    fn events_preserve_every_payload_field(#[case] wire: Value) {
        let event: ResponsesStreamEvent = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(event).unwrap(), wire);
    }

    #[rstest]
    #[case::missing_type(json!({"delta":"hello"}))]
    #[case::numeric_type(json!({"type":7}))]
    #[case::null_type(json!({"type":null}))]
    #[case::non_object(json!([]))]
    fn malformed_events_are_rejected(#[case] wire: Value) {
        assert!(serde_json::from_value::<ResponsesStreamEvent>(wire).is_err());
    }

    #[rstest]
    #[case::flat(json!({"type":"response.create","model":"model-a"}), Some("model-a"))]
    #[case::nested(json!({"type":"response.create","response":{"model":"model-b"}}), Some("model-b"))]
    #[case::none(json!({"type":"response.created","response":{}}), None)]
    fn model_reads_flat_and_nested_shapes(#[case] wire: Value, #[case] expected: Option<&str>) {
        let event: ResponsesStreamEvent = serde_json::from_value(wire).unwrap();
        assert_eq!(event.model(), expected);
    }

    #[cfg(feature = "schema")]
    #[rstest]
    fn event_type_schema_accepts_unknown_strings() {
        let schema = schemars::schema_for!(ResponsesEventType).to_value();
        assert_eq!(schema.get("type"), Some(&json!("string")));
        assert!(schema.get("enum").is_none());
    }
}

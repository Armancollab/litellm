use serde_json::{Map, Value};

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
pub enum ResponsesStreamEventType {
    #[strum(serialize = "response.created")]
    Created,
    #[strum(serialize = "response.in_progress")]
    InProgress,
    #[strum(serialize = "response.completed")]
    Completed,
    #[strum(serialize = "response.failed")]
    Failed,
    #[strum(serialize = "response.incomplete")]
    Incomplete,
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
    #[strum(serialize = "response.function_call_arguments.delta")]
    FunctionCallArgumentsDelta,
    #[strum(serialize = "response.function_call_arguments.done")]
    FunctionCallArgumentsDone,
    #[strum(default, transparent)]
    Other(String),
}

impl ResponsesStreamEventType {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Incomplete)
    }
}

#[macro_rules_attribute::apply(crate::wire_type)]
pub struct ResponsesStreamEvent {
    #[serde(rename = "type")]
    pub event_type: ResponsesStreamEventType,
    #[serde(flatten)]
    pub data: Map<String, Value>,
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;

    #[rstest]
    #[case::in_progress("response.in_progress", ResponsesStreamEventType::InProgress, false)]
    #[case::item_added(
        "response.output_item.added",
        ResponsesStreamEventType::OutputItemAdded,
        false
    )]
    #[case::item_done(
        "response.output_item.done",
        ResponsesStreamEventType::OutputItemDone,
        false
    )]
    #[case::part_added(
        "response.content_part.added",
        ResponsesStreamEventType::ContentPartAdded,
        false
    )]
    #[case::part_done(
        "response.content_part.done",
        ResponsesStreamEventType::ContentPartDone,
        false
    )]
    #[case::annotation(
        "response.output_text.annotation.added",
        ResponsesStreamEventType::OutputTextAnnotationAdded,
        false
    )]
    #[case::text_done(
        "response.output_text.done",
        ResponsesStreamEventType::OutputTextDone,
        false
    )]
    #[case::tool_done(
        "response.function_call_arguments.done",
        ResponsesStreamEventType::FunctionCallArgumentsDone,
        false
    )]
    #[case::created("response.created", ResponsesStreamEventType::Created, false)]
    #[case::completed("response.completed", ResponsesStreamEventType::Completed, true)]
    #[case::failed("response.failed", ResponsesStreamEventType::Failed, true)]
    #[case::incomplete("response.incomplete", ResponsesStreamEventType::Incomplete, true)]
    #[case::text(
        "response.output_text.delta",
        ResponsesStreamEventType::OutputTextDelta,
        false
    )]
    #[case::tool(
        "response.function_call_arguments.delta",
        ResponsesStreamEventType::FunctionCallArgumentsDelta,
        false
    )]
    #[case::unknown("future.event", ResponsesStreamEventType::Other("future.event".into()), false)]
    #[case::empty("", ResponsesStreamEventType::Other(String::new()), false)]
    #[case::case_sensitive("Response.Completed", ResponsesStreamEventType::Other("Response.Completed".into()), false)]
    fn event_types_round_trip_and_classify_terminal_events(
        #[case] wire: &str,
        #[case] expected: ResponsesStreamEventType,
        #[case] terminal: bool,
    ) {
        assert_eq!(
            serde_json::from_value::<ResponsesStreamEventType>(json!(wire)).unwrap(),
            expected
        );
        assert_eq!(serde_json::to_value(&expected).unwrap(), json!(wire));
        assert_eq!(expected.is_terminal(), terminal);
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

    #[cfg(feature = "schema")]
    #[rstest]
    fn event_type_schema_accepts_unknown_strings() {
        let schema = schemars::schema_for!(ResponsesStreamEventType).to_value();
        assert_eq!(schema.get("type"), Some(&json!("string")));
        assert!(schema.get("enum").is_none());
    }
}

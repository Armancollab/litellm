use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, future::ready, stream::BoxStream};
use litellm_framer::{
    SseError, frames,
    sse::{SseBlockCodec, decode_all},
};
use litellm_llms_types::formats::responses::streaming::ResponsesStreamEvent;

use crate::Error;
pub use crate::base_llm::base_model_iterator::ByteStream;

const DONE: &str = "[DONE]";

fn invalid(error: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::InvalidResponse(crate::ErrorDetail::invalid("Responses stream event", error))
}

fn carries_event(block: &Bytes) -> bool {
    decode_all(block).is_ok_and(|events| events.iter().any(|event| event.data != DONE))
}

pub fn openai_sse_blocks(bytes: ByteStream) -> BoxStream<'static, Result<Bytes, SseError>> {
    frames(bytes, SseBlockCodec::default())
        .try_filter(|block| ready(carries_event(block)))
        .boxed()
}

pub fn decode_openai_sse(wire: &[u8]) -> Result<Vec<ResponsesStreamEvent>, Error> {
    decode_all(wire)
        .map_err(|error| {
            Error::InvalidResponse(crate::ErrorDetail::failed("stream framing", error))
        })?
        .iter()
        .filter(|event| event.data != DONE)
        .map(|event| serde_json::from_str(&event.data).map_err(invalid))
        .collect()
}

pub fn encode_openai_sse(event: &ResponsesStreamEvent) -> Result<Bytes, Error> {
    let name = event.event_type.as_ref();
    if name.contains(['\r', '\n']) {
        return Err(Error::InvalidResponse(crate::ErrorDetail::InvalidValue {
            field: "event type",
            expected: "a single line",
            actual: serde_json::Value::String(name.to_owned()),
        }));
    }
    let data = serde_json::to_string(event).map_err(invalid)?;
    Ok(Bytes::from(format!(
        "event: {}\ndata: {data}\n\n",
        event.event_type
    )))
}

#[cfg(test)]
mod tests {
    use futures_util::{StreamExt, TryStreamExt, stream};
    use litellm_llms_types::formats::responses::streaming::ResponsesStreamEventType;
    use rstest::rstest;
    use serde_json::{Value, json};

    use super::*;

    fn in_pieces(wire: &[u8], size: usize) -> ByteStream {
        let pieces: Vec<Bytes> = wire.chunks(size).map(Bytes::copy_from_slice).collect();
        stream::iter(pieces.into_iter().map(Ok)).boxed()
    }

    #[rstest]
    #[case::created(json!({"type":"response.created","response":{"id":"r","output":[]}}))]
    #[case::text(json!({"type":"response.output_text.delta","delta":"hello\nworld","sequence_number":3}))]
    #[case::tool(json!({"type":"response.function_call_arguments.delta","delta":"{\"x\":","item_id":"fc_1"}))]
    #[case::unknown(json!({"type":"future.event","payload":[1,null],"future":{"nested":true}}))]
    #[case::completed(json!({"type":"response.completed","response":{"id":"r","usage":{"input_tokens":3}}}))]
    #[case::failed(json!({"type":"response.failed","response":{"error":{"code":"failure"}}}))]
    #[case::incomplete(json!({"type":"response.incomplete","response":{"incomplete_details":{"reason":"limit"}}}))]
    fn typed_events_round_trip_through_sse_without_losing_fields(#[case] wire: Value) {
        let event: ResponsesStreamEvent = serde_json::from_value(wire.clone()).unwrap();
        let encoded = encode_openai_sse(&event).unwrap();
        let framed = decode_all(&encoded).unwrap();
        assert_eq!(framed.len(), 1);
        assert_eq!(framed[0].event.as_deref(), Some(event.event_type.as_ref()));
        assert_eq!(
            serde_json::from_str::<Value>(&framed[0].data).unwrap(),
            wire
        );
        assert_eq!(decode_openai_sse(&encoded).unwrap(), vec![event]);
    }

    #[rstest]
    #[case::lf("\n")]
    #[case::crlf("\r\n")]
    #[case::cr("\r")]
    #[tokio::test]
    async fn streamed_blocks_preserve_events_and_skip_comments_and_done(
        #[case] newline: &str,
        #[values(1, 5, 4096)] size: usize,
    ) {
        let first = format!(
            "event: response.created{newline}data: {{\"type\":\"response.created\"}}{newline}{newline}"
        );
        let second = format!(
            "event: future.event{newline}data: {{\"type\":\"future.event\",\"payload\":null}}{newline}{newline}"
        );
        let wire = format!(
            "\u{feff}{first}: keepalive{newline}{newline}data: [DONE]{newline}{newline}{second}"
        );
        let blocks = openai_sse_blocks(in_pieces(wire.as_bytes(), size))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(blocks, vec![Bytes::from(first), Bytes::from(second)]);
        let events = decode_openai_sse(wire.as_bytes()).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].event_type, ResponsesStreamEventType::Created);
        assert_eq!(
            events[1].event_type,
            ResponsesStreamEventType::Other("future.event".into())
        );
        assert_eq!(events[1].data.get("payload"), Some(&Value::Null));
    }

    #[rstest]
    #[case::invalid_json(b"data: {\n\n")]
    #[case::missing_type(b"data: {}\n\n")]
    #[case::numeric_type(b"data: {\"type\":3}\n\n")]
    #[case::invalid_utf8(b"data: \xff\n\n")]
    fn malformed_events_return_invalid_response(#[case] wire: &[u8]) {
        assert!(matches!(
            decode_openai_sse(wire),
            Err(Error::InvalidResponse(_))
        ));
    }

    #[rstest]
    #[case::lf("future\nevent")]
    #[case::cr("future\revent")]
    fn event_names_cannot_inject_sse_fields(#[case] name: &str) {
        let event = ResponsesStreamEvent {
            event_type: ResponsesStreamEventType::Other(name.into()),
            data: Default::default(),
        };
        assert!(matches!(
            encode_openai_sse(&event),
            Err(Error::InvalidResponse(_))
        ));
    }

    #[rstest]
    #[tokio::test]
    async fn malformed_json_is_framed_without_being_silently_dropped() {
        let blocks = openai_sse_blocks(in_pieces(b"data: {\n\n", 1))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(blocks, vec![Bytes::from_static(b"data: {\n\n")]);
        assert!(matches!(
            decode_openai_sse(&blocks[0]),
            Err(Error::InvalidResponse(_))
        ));
    }

    #[rstest]
    #[tokio::test]
    async fn framing_preserves_body_errors_after_delivered_events() {
        let mut blocks = openai_sse_blocks(
            stream::iter([
                Ok(Bytes::from_static(
                    b"data: {\"type\":\"response.created\"}\n\n",
                )),
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "reset",
                )),
            ])
            .boxed(),
        );
        assert_eq!(
            blocks.next().await.unwrap().unwrap(),
            Bytes::from_static(b"data: {\"type\":\"response.created\"}\n\n")
        );
        assert!(matches!(blocks.next().await, Some(Err(SseError::Body(_)))));
        assert!(blocks.next().await.is_none());
    }
}

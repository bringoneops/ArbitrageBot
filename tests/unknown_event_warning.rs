use binance_us_and_global::{
    events::StreamMessage,
    handle_stream_event,
};
use tracing_test::traced_test;

#[traced_test]
#[test]
fn warns_on_unknown_event() {
    let raw = r#"{"stream":"test","data":{"e":"mystery"}}"#;
    let msg: StreamMessage<'_> = serde_json::from_str(raw).expect("failed to parse");
    handle_stream_event(&msg, raw);
    assert!(logs_contain("unknown event"));
    assert!(logs_contain("mystery"));
}

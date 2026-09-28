//! The envelope a Stream travels in: one `WireEvent` — the standard Event
//! Grid and Xmip both follow, `CloudEvents` 1.0 — of this transport's
//! type, the Stream its binary data, and the ceilings Event Grid holds an
//! event to.
//!
//! The event itself — its attributes, its JSON format, `data_base64` for
//! bytes — is `xmip-core-event`'s `WireEvent`, written and read there
//! once (ADR-0065 clause 3). This transport carried a second one of its
//! own, with its own JSON writer and reader, until 2026-09-26. What
//! is left here is Event Grid's: the `type` a Stream is published under,
//! how big an event may be, and a Stream's bytes out of an event another
//! publisher wrote.

use event::wire::{Data, WireEvent};
use serde_json::Value;
use transport::error::{Result, protocol_error};
use xcore::{EventId, IdGenerator, UuidV7Generator};

/// The `type` every event this transport publishes carries.
const STREAM_TYPE: &str = "se.xmip.stream";

/// The largest event Event Grid carries, envelope and all: one mebibyte.
pub const EVENT_CEILING: usize = 1024 * 1024;

/// What of that is kept for the envelope — the four attributes and their
/// names, a source URL of ordinary length — so the data has the rest.
pub const ENVELOPE: usize = 1024;

/// A Stream as an event from `source`, under an identity of its own.
#[must_use]
pub fn stream(source: &str, data: &[u8]) -> WireEvent {
    let id = EventId::new(UuidV7Generator.next_u128());
    let mut event = WireEvent::new(id.to_string(), source, STREAM_TYPE);
    event.data = Some(Data::Binary(data.to_vec()));
    event
}

/// The event a JSON value writes, or why it is not one.
///
/// # Errors
/// What `WireEvent::read_json` refuses: not version 1.0, a required
/// attribute missing, `data_base64` that is not base64.
pub fn read(value: &Value) -> Result<WireEvent> {
    WireEvent::read_json(value).map_err(|error| protocol_error(error.to_string()))
}

/// The event in the JSON format, or why it cannot be written.
///
/// # Errors
/// What `WireEvent::json` refuses: data declared JSON that is not.
pub fn json(event: &WireEvent) -> Result<Value> {
    event
        .json()
        .map_err(|error| protocol_error(error.to_string()))
}

/// The Stream an event carries: its data as `Data::bytes` gives it, nothing
/// where it has none.
#[must_use]
pub fn data(event: &WireEvent) -> Vec<u8> {
    event.data.as_ref().map(Data::bytes).unwrap_or_default()
}

/// The event as an origin names it: its source with its id as the
/// fragment.
#[must_use]
pub fn origin(event: &WireEvent) -> String {
    format!("{}#{}", event.source, event.id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_stream_is_written_as_the_schema_wants_it_and_reads_back_whole() {
        let event = stream("http://topic.local/api/events", &[0, 0xff, b'\n']);
        let written = event.json().expect("written");
        assert_eq!(written["specversion"], "1.0");
        assert_eq!(written["type"], STREAM_TYPE);
        assert_eq!(written["data_base64"], "AP8K");
        let read = read(&written).expect("read");
        assert_eq!(read, event);
        assert_eq!(data(&read), [0, 0xff, b'\n']);
        assert_eq!(
            origin(&event),
            format!("http://topic.local/api/events#{}", event.id)
        );
        let other = stream("s", b"");
        assert_ne!(other.id, event.id, "fresh ids");
        assert!(data(&other).is_empty());
    }

    #[test]
    fn another_publishers_data_is_read_and_what_is_not_an_event_is_refused() {
        let text = json!({"specversion": "1.0", "type": "t", "source": "s", "id": "1",
            "datacontenttype": "text/plain", "data": "hi"});
        assert_eq!(data(&read(&text).expect("read")), b"hi");
        let untyped =
            json!({"specversion": "1.0", "type": "t", "source": "s", "id": "1", "data": "hi"});
        assert_eq!(
            data(&read(&untyped).expect("read")),
            br#""hi""#,
            "JSON, untyped"
        );
        let object = json!({"specversion": "1.0", "type": "t", "source": "s", "id": "1",
            "data": {"a": 1}});
        assert_eq!(data(&read(&object).expect("read")), br#"{"a":1}"#);
        let bare = json!({"specversion": "1.0", "type": "t", "source": "s", "id": "1"});
        assert!(data(&read(&bare).expect("read")).is_empty());
        let old = json!({"specversion": "0.3", "type": "t", "source": "s", "id": "1"});
        assert!(read(&old).is_err());
        let nameless = json!({"specversion": "1.0", "type": "t", "source": "s"});
        let refused = read(&nameless).expect_err("no id");
        assert!(refused.message.contains("no id"), "{refused}");
        assert!(!refused.retryable);
        let broken = json!({"specversion": "1.0", "type": "t", "source": "s", "id": "1",
            "data_base64": "not base64!"});
        assert!(read(&broken).is_err());
    }
}

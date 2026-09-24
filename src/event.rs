//! One `CloudEvent`: the envelope a Stream travels in, written and read.
//!
//! Event Grid takes and delivers events in the `CloudEvents` 1.0 schema,
//! one JSON object: `specversion`, `type`, `source`, `id`, and the data.
//! A Stream is bytes, so it travels as `data_base64` — the schema's field
//! for binary data — and comes back the same bytes. An event another
//! publisher wrote with `data` instead is read too: a string as its UTF-8,
//! any other JSON as its text. Both halves are here, so the publisher and
//! the webhook cannot drift.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use codec::base64;
use serde_json::{Value, json};
use transport::error::{Result, protocol_error};

/// The content type one event travels under.
pub const CONTENT_TYPE: &str = "application/cloudevents+json; charset=utf-8";

/// The content type a batch of events travels under.
pub const BATCH_CONTENT_TYPE: &str = "application/cloudevents-batch+json; charset=utf-8";

/// The `type` every event this transport publishes carries.
pub const STREAM_TYPE: &str = "se.xmip.stream";

/// The largest event Event Grid carries, envelope and all: one mebibyte.
pub const EVENT_CEILING: usize = 1024 * 1024;

/// What of that is kept for the envelope — the four fields and their
/// names, a source URL of ordinary length — so the data has the rest.
pub const ENVELOPE: usize = 1024;

/// One event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudEvent {
    pub id: String,
    pub source: String,
    pub kind: String,
    pub data: Vec<u8>,
}

impl CloudEvent {
    /// A Stream as an event from `source`, under a fresh id.
    #[must_use]
    pub fn stream(source: &str, data: &[u8]) -> Self {
        Self {
            id: fresh_id(),
            source: source.to_string(),
            kind: STREAM_TYPE.to_string(),
            data: data.to_vec(),
        }
    }

    /// The event as the schema writes it.
    #[must_use]
    pub fn to_json(&self) -> Value {
        json!({
            "specversion": "1.0",
            "type": self.kind,
            "source": self.source,
            "id": self.id,
            "data_base64": base64::encode(&self.data),
        })
    }

    /// The event `value` writes, or why it is not one.
    ///
    /// # Errors
    /// Where `value` is not a `CloudEvent`: no `specversion` 1.0, no id, no
    /// source, or `data_base64` that is not base64.
    pub fn from_json(value: &Value) -> Result<Self> {
        if value["specversion"].as_str() != Some("1.0") {
            return Err(protocol_error("an event that is not CloudEvents 1.0"));
        }
        let field = |name: &str| {
            value[name]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| protocol_error(format!("an event with no {name}")))
        };
        let data = match (&value["data_base64"], &value["data"]) {
            (Value::String(encoded), _) => base64::decode(encoded)
                .map_err(|e| protocol_error(format!("data_base64 that is not base64: {e}")))?,
            (_, Value::String(text)) => text.as_bytes().to_vec(),
            (_, Value::Null) => Vec::new(),
            (_, other) => other.to_string().into_bytes(),
        };
        Ok(Self {
            id: field("id")?,
            source: field("source")?,
            kind: field("type")?,
            data,
        })
    }

    /// The event as an origin names it: its source with its id as the
    /// fragment.
    #[must_use]
    pub fn origin(&self) -> String {
        format!("{}#{}", self.source, self.id)
    }
}

/// An id no other event from this process carries: the moment and a
/// count.
fn fresh_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let count = NEXT.fetch_add(1, Ordering::Relaxed);
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    format!("{secs:x}-{count:x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_event_is_written_as_the_schema_wants_it_and_reads_back_whole() {
        let event = CloudEvent::stream("http://topic.local/api/events", &[0, 0xff, b'\n']);
        let written = event.to_json();
        assert_eq!(written["specversion"], "1.0");
        assert_eq!(written["type"], STREAM_TYPE);
        assert_eq!(written["data_base64"], "AP8K");
        assert_eq!(CloudEvent::from_json(&written).expect("read"), event);
        assert_eq!(
            event.origin(),
            format!("http://topic.local/api/events#{}", event.id)
        );
        let other = CloudEvent::stream("s", b"");
        assert_ne!(other.id, event.id, "fresh ids");
    }

    #[test]
    fn another_publishers_data_is_read_and_what_is_not_an_event_is_refused() {
        let text =
            json!({"specversion": "1.0", "type": "t", "source": "s", "id": "1", "data": "hi"});
        assert_eq!(CloudEvent::from_json(&text).expect("read").data, b"hi");
        let object = json!({"specversion": "1.0", "type": "t", "source": "s", "id": "1",
            "data": {"a": 1}});
        assert_eq!(
            CloudEvent::from_json(&object).expect("read").data,
            br#"{"a":1}"#
        );
        let bare = json!({"specversion": "1.0", "type": "t", "source": "s", "id": "1"});
        assert!(CloudEvent::from_json(&bare).expect("read").data.is_empty());
        let old = json!({"specversion": "0.3", "type": "t", "source": "s", "id": "1"});
        assert!(CloudEvent::from_json(&old).is_err());
        let nameless = json!({"specversion": "1.0", "type": "t", "source": "s"});
        assert!(
            CloudEvent::from_json(&nameless)
                .expect_err("no id")
                .message
                .contains("no id")
        );
        let broken = json!({"specversion": "1.0", "type": "t", "source": "s", "id": "1",
            "data_base64": "not base64!"});
        assert!(CloudEvent::from_json(&broken).is_err());
    }
}

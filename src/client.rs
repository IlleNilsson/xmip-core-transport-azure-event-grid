//! Xmip's side: the one call a Send Location makes, a request carrying
//! the topic key over one connection to the topic endpoint.
//!
//! A topic endpoint is a whole URL — `https://<topic>.<region>-1.
//! eventgrid.azure.net/api/events` in the cloud, `http://127.0.0.1:port/
//! api/events` for a stand-in — and publishing is one `POST` to it with
//! `aeg-sas-key` naming the key and one `CloudEvent` as the body. The
//! service answers 200 with nothing, or a JSON error naming its code.

use std::time::Duration;

use serde_json::Value;
use transport::error::Result;

use crate::event::{CONTENT_TYPE, CloudEvent};
use http::endpoint;
use http::message::{self, Request, Response};
use http::target::HttpTarget;

/// The header the topic key travels in.
pub const KEY_HEADER: &str = "aeg-sas-key";

/// The API version every publish names.
pub const API_VERSION: &str = "2018-01-01";

pub struct Client {
    key: String,
    timeout: Option<Duration>,
}

impl Client {
    /// Publish with `key`, one of the topic's two access keys.
    #[must_use]
    pub fn new(key: &str) -> Self {
        Self {
            key: key.to_string(),
            timeout: None,
        }
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Publish `event` to the topic at `topic_url`.
    ///
    /// # Errors
    /// Where the URL is not HTTP, or the topic refused or could not be
    /// reached.
    pub fn publish(&self, topic_url: &str, event: &CloudEvent) -> Result<()> {
        let target = HttpTarget::parse(topic_url)?;
        let scheme = if target.secure { "https" } else { "http" };
        let endpoint = format!("{scheme}://{}", target.authority);
        let request = Request::new("POST", target.path)
            .query("api-version", API_VERSION)
            .header("Host", &endpoint::authority(&endpoint)?)
            .header(KEY_HEADER, &self.key)
            .header("Content-Type", CONTENT_TYPE)
            .body(event.to_json().to_string().as_bytes());
        let stream = endpoint::connect(&endpoint, self.timeout)?;
        judge(message::exchange(stream, &request)?).map(|_| ())
    }
}

/// A 2xx answer as it is; anything else as a failure naming the status and
/// the code the service put in the body, retryable where HTTP says come
/// back.
///
/// # Errors
/// Where the status is not 2xx.
pub fn judge(response: Response) -> Result<Response> {
    message::judge("Event Grid", response, code, |_| false)
}

/// The code an error answer names, or nothing.
fn code(response: &Response) -> String {
    serde_json::from_slice::<Value>(&response.body)
        .ok()
        .and_then(|error| error["error"]["code"].as_str().map(str::to_string))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::{Event, Session};
    use serde_json::json;
    use transport::socket;

    #[test]
    fn a_publish_reaches_a_session_and_a_wrong_key_is_refused_with_the_code() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let far_end = std::thread::spawn(move || {
            let mut session = Session::new("key").timing_out_after(Duration::from_secs(2));
            let events: Vec<Event> = (0..2)
                .map(|_| session.serve_one(&listener).expect("served"))
                .collect();
            (session, events)
        });
        let topic = format!("http://{address}/api/events");
        let event = CloudEvent::stream(&topic, b"UNA:+.? '");
        Client::new("key")
            .timing_out_after(Duration::from_secs(2))
            .publish(&topic, &event)
            .expect("published");
        let refused = Client::new("wrong")
            .publish(&topic, &event)
            .expect_err("refused");
        assert_eq!(refused.message, "Event Grid answered 401 Unauthorized");
        assert!(!refused.retryable);
        let (session, events) = far_end.join().expect("thread");
        assert_eq!(session.events(), vec![event.clone()]);
        assert_eq!(events[0], Event::Published(event));
        assert_eq!(events[1], Event::Refused("Unauthorized".to_string()));
        assert!(
            Client::new("k")
                .publish("topic.local", &CloudEvent::stream("s", b""))
                .is_err()
        );
        assert!(
            Client::new("k")
                .publish(
                    "http://127.0.0.1:1/api/events",
                    &CloudEvent::stream("s", b"")
                )
                .expect_err("nobody")
                .retryable
        );
    }

    #[test]
    fn a_server_failure_is_worth_repeating_and_a_client_one_is_not() {
        assert!(judge(Response::new(503)).expect_err("server").retryable);
        assert!(judge(Response::new(429)).expect_err("throttled").retryable);
        let body = json!({"error": {"code": "BadRequest", "message": "no"}}).to_string();
        let failure = judge(Response::new(400).body(body.as_bytes())).expect_err("bad");
        assert!(!failure.retryable);
        assert_eq!(failure.message, "Event Grid answered 400 BadRequest");
        assert_eq!(judge(Response::new(200)).expect("ok").status, 200);
    }
}

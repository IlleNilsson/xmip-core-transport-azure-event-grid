//! The webhook a subscription delivers to: what Event Grid sends it, read
//! and written.
//!
//! Before it delivers anything, Event Grid proves the webhook is willing:
//! for a `CloudEvents` subscription an `OPTIONS` naming its origin in
//! `WebHook-Request-Origin`, answered by naming that origin back in
//! `WebHook-Allowed-Origin`; for an Event Grid schema subscription a
//! `SubscriptionValidation` event carrying a code, answered by echoing it
//! as `validationResponse`. The webhook answers both. Then each event is
//! one `POST` under the `CloudEvents` content type — or a batch under the
//! batch one — answered 200 once taken. Both halves are here — the
//! webhook reads a delivery, the far end in [`crate::Session`] writes one
//! — so the two cannot drift.

use std::net::TcpListener;
use std::time::Duration;

use serde_json::{Value, json};
use transport::error::{Result, TransportError, protocol_error};

use crate::event::{BATCH_CONTENT_TYPE, CONTENT_TYPE, CloudEvent};
use http::endpoint;
use http::message::{self, Request, Response};
use http::server;
use http::target::HttpTarget;

/// What Event Grid delivered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// The abuse-protection handshake, from this origin — answered.
    Handshake(String),
    /// A subscription validation event with this code — echoed.
    Validation(String),
    /// Events, one or a batch.
    Events(Vec<CloudEvent>),
}

/// The delivery `request` carries, and the answer it gets.
///
/// # Errors
/// Where the request is neither handshake nor event: a body that is not
/// JSON, an object that is not a `CloudEvent`, a validation with no code.
pub fn parse(request: &Request) -> Result<(Delivery, Response)> {
    if request.method == "OPTIONS" {
        let origin = request
            .header_value("webhook-request-origin")
            .ok_or_else(|| protocol_error("an OPTIONS naming no WebHook-Request-Origin"))?;
        let answer = Response::new(200)
            .header("WebHook-Allowed-Origin", origin)
            .header("WebHook-Allowed-Rate", "*");
        return Ok((Delivery::Handshake(origin.to_string()), answer));
    }
    let body: Value = serde_json::from_slice(&request.body)
        .map_err(|e| protocol_error(format!("a delivery that is not JSON: {e}")))?;
    if request.header_value("aeg-event-type") == Some("SubscriptionValidation") {
        let code = body[0]["data"]["validationCode"]
            .as_str()
            .ok_or_else(|| protocol_error("a validation with no validationCode"))?;
        let answer = json!({ "validationResponse": code }).to_string();
        let response = Response::new(200)
            .header("Content-Type", "application/json")
            .body(answer.as_bytes());
        return Ok((Delivery::Validation(code.to_string()), response));
    }
    let events = match &body {
        Value::Array(each) => each
            .iter()
            .map(CloudEvent::from_json)
            .collect::<Result<_>>()?,
        one => vec![CloudEvent::from_json(one)?],
    };
    Ok((Delivery::Events(events), Response::new(200)))
}

/// The far end's side: the request Event Grid makes to deliver `event` to
/// a webhook at `path`.
#[must_use]
pub fn deliver(path: &str, event: &CloudEvent) -> Request {
    Request::new("POST", path)
        .header("Content-Type", CONTENT_TYPE)
        .header("aeg-event-type", "Notification")
        .body(event.to_json().to_string().as_bytes())
}

/// The far end's side: the request Event Grid makes to deliver `events`
/// as one batch.
#[must_use]
pub fn deliver_batch(path: &str, events: &[CloudEvent]) -> Request {
    let batch: Vec<Value> = events.iter().map(CloudEvent::to_json).collect();
    Request::new("POST", path)
        .header("Content-Type", BATCH_CONTENT_TYPE)
        .header("aeg-event-type", "Notification")
        .body(Value::Array(batch).to_string().as_bytes())
}

/// The far end's side: the handshake Event Grid opens with, from `origin`.
#[must_use]
pub fn handshake(path: &str, origin: &str) -> Request {
    Request::new("OPTIONS", path)
        .header("WebHook-Request-Origin", origin)
        .header("WebHook-Request-Callback", "")
}

/// Push `request` to the webhook at `webhook_url`, as Event Grid does, and
/// expect it taken.
///
/// # Errors
/// Where the URL is not HTTP, the webhook could not be reached, or it did
/// not answer 2xx — Event Grid retries that, so it is retryable.
pub fn push(webhook_url: &str, request: Request, timeout: Option<Duration>) -> Result<Response> {
    let target = HttpTarget::parse(webhook_url)?;
    let scheme = if target.secure { "https" } else { "http" };
    let request = Request {
        path: target.path.to_string(),
        ..request
    }
    .header("Host", target.authority);
    let stream = endpoint::connect(&format!("{scheme}://{}", target.authority), timeout)?;
    let response = message::exchange(stream, &request)?;
    if (200..300).contains(&response.status) {
        Ok(response)
    } else {
        Err(TransportError::retryable(format!(
            "the webhook answered {}",
            response.status
        )))
    }
}

/// Accept one delivery on `listener`, answer it, and say what it was.
///
/// # Errors
/// Where the connection could not be accepted, broke, or did not carry a
/// delivery — which is answered 400 before the error is returned.
pub fn accept_one(listener: &TcpListener, timeout: Option<Duration>) -> Result<Delivery> {
    server::serve_one(listener, timeout, |request| match parse(request) {
        Ok((delivery, response)) => (Ok(delivery), response),
        Err(failure) => (Err(failure), Response::new(400)),
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use transport::socket;

    fn event(data: &[u8]) -> CloudEvent {
        CloudEvent {
            id: "1".to_string(),
            source: "http://topic.local/api/events".to_string(),
            kind: "t".to_string(),
            data: data.to_vec(),
        }
    }

    #[test]
    fn a_delivery_is_written_as_event_grid_writes_it_and_reads_back() {
        let (delivery, response) = parse(&deliver("/hook", &event(b"a<b"))).expect("read");
        assert_eq!(delivery, Delivery::Events(vec![event(b"a<b")]));
        assert_eq!(response.status, 200);
        let batch = deliver_batch("/hook", &[event(b"1"), event(b"2")]);
        assert_eq!(
            parse(&batch).expect("read").0,
            Delivery::Events(vec![event(b"1"), event(b"2")])
        );
        let (delivery, response) = parse(&handshake("/hook", "eventgrid.azure.net")).expect("h");
        assert_eq!(
            delivery,
            Delivery::Handshake("eventgrid.azure.net".to_string())
        );
        assert_eq!(
            response.header_value("webhook-allowed-origin"),
            Some("eventgrid.azure.net")
        );
        let validation = Request::new("POST", "/hook")
            .header("aeg-event-type", "SubscriptionValidation")
            .body(br#"[{"data":{"validationCode":"c0de"}}]"#);
        let (delivery, response) = parse(&validation).expect("v");
        assert_eq!(delivery, Delivery::Validation("c0de".to_string()));
        assert_eq!(response.text(), r#"{"validationResponse":"c0de"}"#);
    }

    #[test]
    fn what_is_not_a_delivery_is_refused_with_the_reason() {
        assert!(parse(&Request::new("OPTIONS", "/")).is_err());
        assert!(
            parse(&Request::new("POST", "/").body(b"not json"))
                .expect_err("not JSON")
                .message
                .contains("JSON")
        );
        let bare = Request::new("POST", "/")
            .header("aeg-event-type", "SubscriptionValidation")
            .body(b"[{}]");
        assert!(
            parse(&bare)
                .expect_err("no code")
                .message
                .contains("validationCode")
        );
        assert!(parse(&Request::new("POST", "/").body(b"{}")).is_err());
    }

    #[test]
    fn a_pushed_delivery_is_accepted_on_loopback_and_a_bad_one_answered_400() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let timeout = Some(Duration::from_secs(2));
        let webhook = std::thread::spawn(move || {
            let first = accept_one(&listener, timeout);
            let second = accept_one(&listener, timeout);
            (first, second)
        });
        let url = format!("http://{address}/hook");
        push(&url, deliver("/", &event(b"a")), timeout).expect("pushed");
        let refused = push(&url, Request::new("POST", "/").body(b"x"), timeout).expect_err("400");
        assert!(refused.retryable, "Event Grid retries a 400");
        let (first, second) = webhook.join().expect("thread");
        assert_eq!(
            first.expect("a delivery"),
            Delivery::Events(vec![event(b"a")])
        );
        assert!(second.is_err(), "not a delivery");
        assert!(push("hook.local/x", deliver("/", &event(b"a")), timeout).is_err());
    }
}

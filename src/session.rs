//! The far end: enough of Event Grid to take one Location's events and
//! deliver them, and what a test or the playground puts on loopback.
//!
//! Not Event Grid. One session stands as a topic: it checks every publish
//! for one key, reads the `CloudEvent` or the batch, holds what was
//! published in memory and answers 200 as the service does, or the JSON
//! error with its code. Delivering to a webhook is [`Session::deliver`],
//! and opening with the handshake is [`Session::validate`], which a test
//! calls where Event Grid would push on its own.

use std::net::TcpListener;
use std::time::Duration;

use serde_json::{Value, json};
use transport::error::{Result, protocol_error};

use crate::client::{API_VERSION, KEY_HEADER};
use crate::event::{CloudEvent, EVENT_CEILING};
use crate::webhook;
use http::message::{Request, Response};
use http::server;

/// What the client did, as [`Session::serve_one`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client published this event — the first of a batch, where it
    /// was one.
    Published(CloudEvent),
    /// The client was answered with this error code.
    Refused(String),
}

pub struct Session {
    key: String,
    events: Vec<CloudEvent>,
    timeout: Option<Duration>,
}

impl Session {
    /// Answer requests presenting `key`.
    #[must_use]
    pub fn new(key: &str) -> Self {
        Self {
            key: key.to_string(),
            events: Vec::new(),
            timeout: None,
        }
    }

    /// Give up on a client that stops mid-request after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Every event published so far, in order.
    #[must_use]
    pub fn events(&self) -> Vec<CloudEvent> {
        self.events.clone()
    }

    /// Accept one connection on `listener`, answer its one request, and say
    /// what it was.
    ///
    /// # Errors
    /// Where the connection could not be accepted, broke, or sent nothing.
    pub fn serve_one(&mut self, listener: &TcpListener) -> Result<Event> {
        server::serve_one(listener, self.timeout, |request| self.answer(request))
    }

    /// Open with the handshake at the webhook at `webhook_url`, as Event
    /// Grid does before its first delivery, and expect the origin allowed.
    ///
    /// # Errors
    /// Where the webhook could not be reached or did not allow the origin.
    pub fn validate(&self, webhook_url: &str) -> Result<()> {
        let origin = "eventgrid.azure.net";
        let answer = webhook::push(webhook_url, webhook::handshake("/", origin), self.timeout)?;
        match answer.header_value("webhook-allowed-origin") {
            Some(allowed) if allowed == origin || allowed == "*" => Ok(()),
            _ => Err(protocol_error("the webhook did not allow the origin")),
        }
    }

    /// Deliver `event` to the webhook at `webhook_url`, as Event Grid does.
    ///
    /// # Errors
    /// Where the webhook could not be reached or did not take it.
    pub fn deliver(&self, webhook_url: &str, event: &CloudEvent) -> Result<()> {
        webhook::push(webhook_url, webhook::deliver("/", event), self.timeout).map(|_| ())
    }

    fn answer(&mut self, request: &Request) -> (Event, Response) {
        if request.header_value(KEY_HEADER) != Some(self.key.as_str()) {
            return refused(
                401,
                "Unauthorized",
                "The request authorization key is not valid",
            );
        }
        if request.method != "POST" || !request.path.ends_with("/api/events") {
            return refused(404, "NotFound", "Not a topic endpoint");
        }
        if request.query_value("api-version") != Some(API_VERSION) {
            return refused(
                400,
                "BadRequest",
                "An api-version this session does not speak",
            );
        }
        let body: Value = match serde_json::from_slice(&request.body) {
            Ok(body) => body,
            Err(e) => return refused(400, "BadRequest", &format!("Not JSON: {e}")),
        };
        let each = match &body {
            Value::Array(each) => each.clone(),
            one => vec![one.clone()],
        };
        let mut published = Vec::with_capacity(each.len());
        for value in &each {
            if value.to_string().len() > EVENT_CEILING {
                let message = format!("An event is at most {EVENT_CEILING} bytes");
                return refused(413, "RequestEntityTooLarge", &message);
            }
            match CloudEvent::from_json(value) {
                Ok(event) => published.push(event),
                Err(failure) => return refused(400, "BadRequest", &failure.message),
            }
        }
        let Some(first) = published.first().cloned() else {
            return refused(400, "BadRequest", "A batch with no events");
        };
        self.events.extend(published);
        (Event::Published(first), Response::new(200))
    }
}

fn refused(status: u16, code: &str, message: &str) -> (Event, Response) {
    let body = json!({ "error": { "code": code, "message": message } });
    let response = Response::new(status)
        .header("Content-Type", "application/json")
        .body(body.to_string().as_bytes());
    (Event::Refused(code.to_string()), response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::CONTENT_TYPE;

    fn publish(key: &str, body: &[u8]) -> Request {
        Request::new("POST", "/api/events")
            .query("api-version", API_VERSION)
            .header(KEY_HEADER, key)
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
    }

    #[test]
    fn a_session_takes_one_event_or_a_batch_and_refuses_in_event_grids_shapes() {
        let mut session = Session::new("key");
        let event = CloudEvent::stream("http://topic.local/api/events", b"a<b");
        let (taken, response) =
            session.answer(&publish("key", event.to_json().to_string().as_bytes()));
        assert_eq!(response.status, 200);
        assert_eq!(taken, Event::Published(event.clone()));
        let batch = Value::Array(vec![event.to_json(), event.to_json()]).to_string();
        let (taken, _) = session.answer(&publish("key", batch.as_bytes()));
        assert_eq!(taken, Event::Published(event.clone()));
        assert_eq!(session.events().len(), 3);
        let (taken, response) = session.answer(&publish("wrong", b"{}"));
        assert_eq!(
            (taken, response.status),
            (Event::Refused("Unauthorized".to_string()), 401)
        );
        assert!(response.text().contains("not valid"));
        let (_, response) = session.answer(&publish("key", b"not json"));
        assert_eq!(response.status, 400);
        let (_, response) = session.answer(&publish("key", b"[]"));
        assert_eq!(response.status, 400);
        let (_, response) = session.answer(&publish("key", br#"{"specversion":"0.3"}"#));
        assert_eq!(response.status, 400);
        let too_big = CloudEvent::stream("s", &vec![0; EVENT_CEILING])
            .to_json()
            .to_string();
        let (taken, response) = session.answer(&publish("key", too_big.as_bytes()));
        assert_eq!(
            (taken, response.status),
            (Event::Refused("RequestEntityTooLarge".to_string()), 413)
        );
        let elsewhere = publish("key", b"{}");
        let (_, response) = session.answer(&Request {
            path: "/elsewhere".to_string(),
            ..elsewhere
        });
        assert_eq!(response.status, 404);
        let (_, response) = session.answer(&Request {
            query: vec![("api-version".to_string(), "2010-01-01".to_string())],
            ..publish("key", b"{}")
        });
        assert_eq!(response.status, 400);
    }
}

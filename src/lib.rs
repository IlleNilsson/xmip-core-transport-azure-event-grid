#![forbid(unsafe_code)]

//! Streams that travel as `CloudEvents` through an Event Grid topic. One
//! event is one Stream, its id kept beside it.
//!
//! Event Grid is the event router of every organisation that lives in
//! Azure: a topic, and subscriptions that deliver what is published to it
//! — to a webhook, among other places. A Send Location publishes a Stream
//! as one `CloudEvent` to a topic endpoint, with the topic key over plain
//! HTTP/1.1 on a socket — `https://` with the `tls` feature, which is the
//! http technology's TLS (ADR-0033). A Receive Location is the webhook a
//! subscription delivers to: it answers the validation handshake Event
//! Grid opens with, then takes each event as a Stream.
//!
//! ```text
//! event.rs      one `CloudEvent`, written and read
//! client.rs     Xmip's side: publish
//! webhook.rs    the endpoint a subscription delivers to, and what it delivers
//! session.rs    the far end a test or the playground runs on loopback
//! ```
//!
//! The endpoint, the target parser, HTTP itself and the judgement of an
//! answer come from the http technology (ADR-0044).
//!
//! A Stream is bytes and travels as the event's `data_base64`, so nothing
//! is refused for its content. An event is one mebibyte at most, envelope
//! and all, and base64 is four bytes for three: [`ceiling`] is what that
//! leaves for the data.
//!
//! A topic is not an artefact anyone claims, so [`Transport::claims`]
//! answers `None`. The origin URI is the event's source — the topic
//! endpoint it was published to — with the event id as its fragment. A
//! send target is a topic endpoint URL, or empty for this transport's own.
//!
//! The transport is its own far end (ADR-0051): [`Loopback`] stands the
//! session up as the topic, takes the one publish, opens the handshake
//! with the webhook this transport listens as, and delivers to it.

pub mod client;
pub mod event;
pub mod session;
pub mod webhook;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{API_VERSION, Client, KEY_HEADER};
pub use event::{CloudEvent, ENVELOPE, EVENT_CEILING};
use http::target::HttpTarget;
pub use session::{Event, Session};
use transport::error::{Result, TransportError, protocol_error};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::socket;
use transport::{Arrived, Directions, Transport};
pub use webhook::Delivery;

/// The largest Stream one event carries: what a mebibyte leaves for the
/// data once the envelope has its share and base64 has taken four bytes
/// for three.
#[must_use]
pub const fn ceiling() -> usize {
    (EVENT_CEILING - ENVELOPE) / 4 * 3
}

/// What the loopback pair agrees on: one topic key.
const LOOPBACK_KEY: &str = "probe";

#[derive(Clone)]
pub struct EventGridTransport {
    topic_url: String,
    key: String,
    bind: String,
    timeout: Option<Duration>,
}

impl EventGridTransport {
    /// Publish to the topic endpoint at `topic_url` — `https://<topic>.
    /// <region>-1.eventgrid.azure.net/api/events` in the cloud,
    /// `http://host:port/api/events` for a stand-in.
    #[must_use]
    pub fn new(topic_url: impl Into<String>) -> Self {
        Self {
            topic_url: topic_url.into(),
            key: String::new(),
            bind: "127.0.0.1:0".to_string(),
            timeout: None,
        }
    }

    /// Present this topic key.
    #[must_use]
    pub fn with_key(mut self, key: &str) -> Self {
        self.key = key.to_string();
        self
    }

    /// Listen for deliveries at `bind` — the address the subscription's
    /// webhook URL resolves to.
    #[must_use]
    pub fn listening_at(mut self, bind: &str) -> Self {
        self.bind = bind.to_string();
        self
    }

    /// Give up on an endpoint that stops answering after `timeout`.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// The client this transport publishes through.
    #[must_use]
    pub fn client(&self) -> Client {
        let client = Client::new(&self.key);
        match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        }
    }

    /// A far end that expects this transport's key, for a test or the
    /// playground to run on loopback.
    #[must_use]
    pub fn session(&self) -> Session {
        let session = Session::new(&self.key);
        match self.timeout {
            Some(timeout) => session.timing_out_after(timeout),
            None => session,
        }
    }

    /// Bind the webhook and report the address actually assigned.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.bind)
    }

    /// Take one delivery off an already-bound listener: the events it
    /// carried as Streams; a handshake or a validation, answered, as
    /// nothing yet.
    ///
    /// # Errors
    /// Where the connection failed or the delivery was not one.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Vec<Arrived>> {
        match webhook::accept_one(listener, self.timeout)? {
            Delivery::Events(events) => Ok(events
                .into_iter()
                .map(|event| Arrived::new(event.origin(), event.data))
                .collect()),
            Delivery::Handshake(_) | Delivery::Validation(_) => Ok(Vec::new()),
        }
    }

    /// The topic a target names, or this transport's own where it names
    /// none.
    fn resolve<'a>(&'a self, target: &'a str) -> &'a str {
        if target.is_empty() {
            &self.topic_url
        } else {
            target
        }
    }
}

impl Transport for EventGridTransport {
    fn name(&self) -> &'static str {
        "azure-event-grid"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// One delivery: the events it carried, or nothing where it was the
    /// handshake, now answered.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let (listener, _) = self.bind()?;
        self.accept_one(&listener)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() > ceiling() {
            return Err(TransportError::permanent(format!(
                "{} bytes is over the {} one Event Grid event carries",
                bytes.len(),
                ceiling()
            )));
        }
        let topic_url = self.resolve(target);
        self.client()
            .publish(topic_url, &CloudEvent::stream(topic_url, bytes))
    }
}

impl EventGridTransport {
    /// Both ends on this machine: the session stands as the topic on an
    /// ephemeral local port, the webhook listens on another, one key, the
    /// loopback timeout on every side.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("http://127.0.0.1:0/api/events")
            .with_key(LOOPBACK_KEY)
            .timing_out_after(LOOPBACK_TIMEOUT)
    }
}

/// A session listening for its one publish, and the webhook it then
/// validates and delivers to: the far end is the topic and the
/// subscription both, so what comes back went through the topic and
/// arrived the way a Receive Location takes it.
struct Serving {
    transport: EventGridTransport,
    session: Session,
    listener: TcpListener,
    address: String,
    webhook: TcpListener,
    webhook_address: String,
}

impl FarEnd for Serving {
    fn address(&self) -> &str {
        &self.address
    }

    fn take_one(mut self: Box<Self>) -> Result<Arrived> {
        let published = match self.session.serve_one(&self.listener)? {
            Event::Published(event) => event,
            Event::Refused(code) => {
                return Err(protocol_error(format!("the session refused: {code}")));
            }
        };
        let Serving {
            transport,
            session,
            webhook,
            webhook_address,
            ..
        } = *self;
        let taking = std::thread::spawn(move || {
            let mut taken = transport.accept_one(&webhook)?;
            if taken.is_empty() {
                taken = transport.accept_one(&webhook)?;
            }
            Ok::<_, TransportError>(taken)
        });
        let url = format!("http://{webhook_address}/hook");
        let delivered = session
            .validate(&url)
            .and_then(|()| session.deliver(&url, &published));
        if delivered.is_err() {
            // The poke only has to be quick, because the webhook bounds its own wait. An
            // unbounded poke under port exhaustion waited on Windows' ~21-second SYN
            // schedule; it was bare until 2026-09-21.
            drop(socket::connect_tcp(
                &webhook_address,
                Some(Duration::from_millis(250)),
            ));
        }
        let taken = taking
            .join()
            .map_err(|_| protocol_error("the webhook's thread panicked"))?;
        delivered?;
        taken?
            .into_iter()
            .next()
            .ok_or_else(|| protocol_error("delivered, but the webhook took nothing"))
    }
}

impl Loopback for EventGridTransport {
    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    /// The session bound at the topic URL's authority — `127.0.0.1:0` for
    /// the loopback — and the webhook bound where this transport listens.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let (listener, address) = socket::bind_tcp(HttpTarget::parse(&self.topic_url)?.authority)?;
        let (webhook, webhook_address) = self.bind()?;
        Ok(Box::new(Serving {
            transport: self.clone(),
            session: self.session(),
            listener,
            address,
            webhook,
            webhook_address,
        }))
    }

    /// Publish the payload as one event, from a fresh near end presenting
    /// this transport's key, to the topic on `address`.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        let near = Self {
            topic_url: format!("http://{address}/api/events"),
            ..self.clone()
        };
        near.send("", payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(topic_url: &str, key: &str) -> EventGridTransport {
        EventGridTransport::new(topic_url)
            .with_key(key)
            .timing_out_after(Duration::from_secs(2))
    }

    #[test]
    fn what_is_published_is_delivered_to_the_validated_webhook_and_received() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let topic = format!("http://{address}/api/events");
        let near = node(&topic, "key");
        let (webhook, webhook_address) = near.bind().expect("bind the webhook");
        let webhook_url = format!("http://{webhook_address}/hook");
        let mut session = near.session();
        let far_end = std::thread::spawn(move || {
            // One publish; then the handshake, and the delivery.
            let published = session.serve_one(&listener).expect("served");
            session.validate(&webhook_url).expect("validated");
            let event = session.events().into_iter().next().expect("one event");
            session.deliver(&webhook_url, &event).expect("delivered");
            published
        });
        near.send("", &[0, 0xff, b'\r', b'\n'])
            .expect("its own topic");
        assert!(near.accept_one(&webhook).expect("handshake").is_empty());
        let arrived = near.accept_one(&webhook).expect("delivered");
        assert_eq!(arrived.len(), 1);
        assert_eq!(arrived[0].bytes, [0, 0xff, b'\r', b'\n']);
        assert!(arrived[0].origin_uri.starts_with(&format!("{topic}#")));
        let published = far_end.join().expect("thread");
        assert!(
            matches!(published, Event::Published(event) if event.origin() == arrived[0].origin_uri)
        );
    }

    #[test]
    fn a_wrong_key_is_refused_with_event_grids_own_status_and_code() {
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let mut session = node("http://x/api/events", "key").session();
        let far_end = std::thread::spawn(move || session.serve_one(&listener).expect("served"));
        let failure = node(&format!("http://{address}/api/events"), "wrong")
            .send("", b"x")
            .expect_err("refused");
        assert!(failure.message.contains("401 Unauthorized"), "{failure}");
        assert!(!failure.retryable);
        assert_eq!(
            far_end.join().expect("thread"),
            Event::Refused("Unauthorized".to_string())
        );
    }

    #[test]
    fn a_topic_is_not_claimed_and_an_unreachable_endpoint_is_retryable() {
        let near = node("http://127.0.0.1:1/api/events", "key");
        assert!(near.claims().is_none());
        assert_eq!(near.name(), "azure-event-grid");
        assert!(near.directions().receives() && near.directions().sends());
        assert!(
            near.send("", b"x")
                .expect_err("nothing listening")
                .retryable
        );
        assert!(
            !node("topic.local/api/events", "k")
                .send("", b"x")
                .expect_err("no scheme")
                .retryable
        );
        assert!(
            node("http://x/api/events", "k")
                .listening_at("not an address")
                .receive()
                .is_err()
        );
    }

    #[test]
    fn what_is_over_the_ceiling_is_refused_before_the_wire_with_the_reason() {
        let near = node("http://127.0.0.1:1/api/events", "key");
        let over = vec![b'x'; ceiling() + 1];
        let failure = near.send("", &over).expect_err("over the ceiling");
        assert!(!failure.retryable);
        assert!(
            failure.message.contains(&ceiling().to_string()),
            "{failure}"
        );
        assert_eq!(ceiling(), 785_664);
        assert!(ceiling() / 3 * 4 + ENVELOPE <= EVENT_CEILING);
    }

    #[test]
    fn a_loopback_round_publishes_and_takes_the_delivery_at_the_webhook() {
        let grid = EventGridTransport::loopback();
        let arrived = grid.round(b"UNA:+.? '").expect("round");
        assert_eq!(arrived.bytes, b"UNA:+.? '");
        assert!(
            arrived.origin_uri.starts_with("http://127.0.0.1:"),
            "{}",
            arrived.origin_uri
        );
        assert!(
            arrived.origin_uri.contains("/api/events#"),
            "{}",
            arrived.origin_uri
        );
        assert_eq!(grid.name(), "azure-event-grid");
        assert_eq!(grid.ceiling(), Some(ceiling()));
        assert!(grid.refuses(&[0xff]).is_none());
    }

    /// The Playground's edge payloads, written here so the crate does not
    /// depend on it, and one at the brim.
    fn edge_payloads() -> Vec<(&'static str, Vec<u8>)> {
        vec![
            ("empty", Vec::new()),
            ("one byte", vec![0x2a]),
            ("every byte", (0..=255).collect()),
            ("nul run", vec![0; 512]),
            ("high bytes", vec![0xff; 512]),
            ("crlf storm", b"\r\n".repeat(400)),
            ("the brim", vec![b'x'; ceiling()]),
        ]
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole_and_refuses_over_the_brim() {
        let grid = EventGridTransport::loopback();
        for (name, payload) in edge_payloads() {
            assert!(grid.refuses(&payload).is_none(), "{name}");
            let arrived = grid.round(&payload).expect(name);
            assert_eq!(arrived.bytes, payload, "{name}");
        }
        let over = vec![b'x'; ceiling() + 1];
        let failure = grid.round(&over).expect_err("over the brim");
        assert!(failure.message.starts_with("send failed:"), "{failure}");
        assert!(failure.message.contains("785664"), "{failure}");
    }
}

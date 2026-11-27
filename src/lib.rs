#![forbid(unsafe_code)]

//! Streams that travel as wire events through an Event Grid topic. One
//! event is one Stream, its id kept beside it.
//!
//! Event Grid is the event router of every organization that lives in
//! Azure: a topic, and subscriptions that deliver what is published to it
//! — to a webhook, among other places. A Send Location publishes a Stream
//! as one `WireEvent` to a topic endpoint, with the topic key over plain
//! HTTP/1.1 on a socket — `https://` with the `tls` feature, which is the
//! http technology's TLS (ADR-0033). A Receive Location is the webhook a
//! subscription delivers to: it answers the validation handshake Event
//! Grid opens with, then takes each event as a Stream.
//!
//! ```text
//! envelope.rs   a Stream as one `WireEvent`, and the ceilings an event keeps
//! client.rs     Xmip's side: publish
//! webhook.rs    the endpoint a subscription delivers to, and what it delivers
//! session.rs    the far end a test or the System Tests run on loopback
//! loopback.rs   both ends of one exchange on this machine (ADR-0051)
//! ```
//!
//! The endpoint, the target parser, HTTP itself and the judgment of an
//! answer come from the http technology (ADR-0044).
//!
//! A Stream is bytes and travels as the event's `data_base64`, so nothing
//! is refused for its content. An event is one mebibyte at most, envelope
//! and all, and base64 is four bytes for three: [`ceiling`] is what that
//! leaves for the data.
//!
//! A topic is not an artifact anyone claims, so [`Transport::claims`]
//! answers `None`. The origin URI is the event's source — the topic
//! endpoint it was published to — with the event id as its fragment. A
//! send target is a topic endpoint URL, or empty for this transport's own.
//!
//! The transport is its own far end (ADR-0051): its `Loopback` stands the
//! session up as the topic, takes the one publish, opens the handshake
//! with the webhook this transport listens as, and delivers to it.
//!
//! **Built with its `declaration` feature alone** the crate is
//! [`declaration`].

#[cfg(feature = "default")]
mod batch;
#[cfg(feature = "default")]
pub mod client;
#[cfg(feature = "declaration")]
pub mod declaration;
#[cfg(feature = "default")]
pub mod envelope;
#[cfg(feature = "default")]
mod loopback;
#[cfg(feature = "default")]
pub mod session;
#[cfg(feature = "default")]
pub mod webhook;

#[cfg(feature = "default")]
use std::net::TcpListener;
#[cfg(feature = "default")]
use std::time::Duration;

#[cfg(feature = "default")]
pub use client::{API_VERSION, Client, KEY_HEADER};
#[cfg(feature = "default")]
pub use envelope::{ENVELOPE, EVENT_CEILING};
#[cfg(feature = "default")]
use http::{endpoint::Connections, inbound::Inbound, server};
#[cfg(feature = "default")]
use net::ceiling;
#[cfg(feature = "default")]
pub use session::{Event, Session};
#[cfg(feature = "default")]
use transport::error::Result;
#[cfg(feature = "default")]
use transport::socket;
#[cfg(feature = "default")]
use transport::{Arrived, Configured, Directions, Taken, Transport};
#[cfg(feature = "default")]
pub use webhook::Delivery;
#[cfg(feature = "default")]
use xcore::settings::{Read, Settings};

/// The largest Stream one event carries: what a mebibyte leaves for the
/// data once the envelope has its share and base64 has taken four bytes
/// for three.
#[cfg(feature = "default")]
#[must_use]
pub const fn ceiling() -> usize {
    (EVENT_CEILING - ENVELOPE) / 4 * 3
}

#[cfg(feature = "default")]
#[derive(Clone)]
pub struct EventGridTransport {
    topic_url: String,
    key: String,
    bind: String,
    timeout: Option<Duration>,
    /// The connections kept to the service, shared by every client this
    /// makes.
    connections: Connections,
    /// The webhook's listener a Receive Location keeps, and the
    /// connections Event Grid keeps on it.
    inbound: Inbound,
    /// The secret a Send Location names, the topic key it publishes with,
    /// read at each request (ADR-0019, amendment 2026-10-29; ADR-0015,
    /// amendment 2026-11-25); `key` where it names none.
    secret: Option<transport::Secured>,
}

#[cfg(feature = "default")]
impl EventGridTransport {
    /// Publish to the topic endpoint at `topic_url` — `https://<topic>.
    /// <region>-1.eventgrid.azure.net/api/events` at Azure,
    /// `http://host:port/api/events` for a stand-in.
    #[must_use]
    pub fn new(topic_url: impl Into<String>) -> Self {
        Self {
            topic_url: topic_url.into(),
            key: String::new(),
            bind: "127.0.0.1:0".to_string(),
            timeout: None,
            connections: Connections::new(),
            inbound: Inbound::new(),
            secret: None,
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

    /// The client this transport publishes through, presenting the topic
    /// key the Location's secret holds, read now, or this transport's own
    /// where it names none.
    ///
    /// # Errors
    /// Where the secret is not set, or is not a password.
    pub fn client(&self) -> Result<Client> {
        let key = transport::secured::presented(self.secret.as_ref(), &self.key, self.name())?;
        let client = Client::new(&key).sharing(self.connections.clone());
        Ok(match self.timeout {
            Some(timeout) => client.timing_out_after(timeout),
            None => client,
        })
    }

    /// A far end that expects this transport's key, for a test or the
    /// System Tests to run on loopback.
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

    /// Take one delivery off an already-bound listener and answer it at
    /// once, as a far end does: the events it carried; a handshake or a
    /// validation, answered, as nothing yet.
    ///
    /// # Errors
    /// Where the connection failed or the delivery was not one.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Vec<Taken>> {
        let clock = std::sync::Arc::clone(self.connections.clock());
        Ok(carried(webhook::accept_one(listener, self.timeout, clock)?)
            .into_iter()
            .map(|(origin, data)| Taken::new(origin, data))
            .collect())
    }

    /// The topic a target names, or this transport's own where it names
    /// none (`transport::destination::named`).
    fn resolve<'a>(&'a self, target: &'a str) -> &'a str {
        transport::destination::named(target).unwrap_or(&self.topic_url)
    }
}

/// What a delivery carries: each event's origin and data; a handshake or
/// a validation, answered, as nothing yet.
#[cfg(feature = "default")]
fn carried(delivery: Delivery) -> Vec<(String, Vec<u8>)> {
    match delivery {
        Delivery::Events(events) => events
            .iter()
            .map(|event| (envelope::origin(event), envelope::data(event)))
            .collect(),
        Delivery::Handshake(_) | Delivery::Validation(_) => Vec::new(),
    }
}

#[cfg(feature = "default")]
impl Transport for EventGridTransport {
    fn name(&self) -> &'static str {
        "azure-event-grid"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// The callers turned away at their own connection (`transport::serving`).
    fn turned_away(&self) -> transport::Turned {
        self.inbound.turned_away()
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::answer::ONE_AT_A_TIME
    }

    fn waiting(&self) -> transport::Waiting {
        transport::Waiting::Waits("the listener waits for a delivery")
    }

    /// One delivery: the events it carried, or nothing where it was the
    /// handshake, now answered. Taken from whichever connection Event Grid
    /// posts on first, on the listener its open bound and kept.
    ///
    /// Event Grid waits for its answer to a delivery of events until every
    /// one has its verdict (`batch::status`): `200` where all were
    /// accepted; `503` where any failed, and Event Grid delivers the batch
    /// again by its retry policy; otherwise, where any was refused, `401` or
    /// `400`, which Event Grid does not retry. The handshake is answered at
    /// once.
    fn receive_up_to(&self, _most: usize) -> Result<Vec<Arrived>> {
        let (delivery, reply) = self.inbound.next(self.timeout, |request, peer| {
            let sender = server::Sender::of(&request, peer);
            webhook::hear(&request).map(|delivery| (delivery, sender))
        })?;
        let (delivery, sender) = delivery;
        let carried = carried(delivery?);
        let Some(reply) = reply else {
            return Ok(Vec::new());
        };
        let acknowledgements = batch::acknowledgements(reply, carried.len());
        Ok(carried
            .into_iter()
            .zip(acknowledgements)
            .map(|((origin, data), acknowledgement)| {
                sender
                    .clone()
                    .on(Arrived::whole(origin, data, acknowledgement))
            })
            .collect())
    }

    fn sending(&self) -> transport::Sending {
        transport::Sending::Gathered(
            "one Event Grid event carries the whole body inside its JSON envelope",
        )
    }

    fn send_from(&self, target: &str, outgoing: transport::Outgoing<'_>) -> Result<()> {
        ceiling::within(outgoing.len(), ceiling(), "one Event Grid event carries")?;
        let bytes: &[u8] = &outgoing.gather()?;
        let topic_url = self.resolve(target);
        self.client()?
            .publish(topic_url, &envelope::stream(topic_url, bytes))
    }
}

#[cfg(feature = "default")]
impl Configured for EventGridTransport {
    const SETTINGS: &'static Settings = declaration::SETTINGS;
    const SCHEMES: &'static [&'static str] = declaration::SCHEMES;

    fn timeout_kept(&mut self) -> Option<&mut dyn transport::configured::TimeoutKept> {
        Some(&mut self.timeout)
    }

    fn configured(address: &str, settings: &Read) -> Result<Self> {
        // The topic key is the Location's secret (`secured_by`).
        let mut transport = Self::new(address);
        if let Some(listen) = settings.optional_text("listen") {
            transport = transport.listening_at(listen);
        }
        Ok(transport)
    }

    /// Each caller's turn on the webhook's listener taking at most `turn`.
    fn turning_within(mut self, turn: Duration) -> Self {
        self.inbound = self.inbound.turning_within(turn);
        self
    }

    /// Each callers turn on a Receive Location, and an HTTP/2 peers frames
    /// in a span both ways, counted on clock.
    fn timed_by(mut self, clock: &std::sync::Arc<dyn xcore::Clock>) -> Self {
        self.inbound = self.inbound.timed_by(clock);
        self.connections = self.connections.timed_by(clock);
        self
    }

    /// The webhook every receive takes from, bound where a Receive
    /// Location listens ([`EventGridTransport::bind`]).
    fn listening(&self) -> Result<Option<&str>> {
        self.inbound.bound(|| self.bind()).map(Some)
    }

    /// The topic key the Location's secret holds, presented in
    /// `aeg-sas-key` with each publish. A Receive Location presents
    /// nothing: it is the webhook Event Grid connects to.
    fn secured_by(mut self, secret: transport::Secured) -> Result<Self> {
        self.secret = Some(secret);
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;
    use transport::loopback::Loopback;
    use xcore::settings::Applies;

    fn node(topic_url: &str, key: &str) -> EventGridTransport {
        EventGridTransport::new(topic_url)
            .with_key(key)
            .timing_out_after(Duration::from_secs(2))
    }

    #[test]
    fn event_grid_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(
            EventGridTransport::SETTINGS.problems(),
            Vec::<String>::new()
        );
        let text = |name: &str, value: &str| (name.to_string(), Given::Text(value.to_string()));
        let topic = "https://orders.westeurope-1.eventgrid.azure.net/api/events";
        let sent = EventGridTransport::open(topic, Applies::Send, &[text("timeout", "5s")])
            .expect("built");
        assert_eq!(sent.topic_url, topic);
        assert_eq!(sent.timeout, Some(Duration::from_secs(5)));
        assert!(sent.key.is_empty(), "the key is the Location's secret");
        let received =
            EventGridTransport::open(topic, Applies::Receive, &[text("listen", "0.0.0.0:8443")])
                .expect("built");
        assert_eq!(received.bind, "0.0.0.0:8443");
        let Err(refused) = EventGridTransport::open(topic, Applies::Receive, &[]) else {
            panic!("a Receive Location says where it listens");
        };
        assert!(refused.message.contains("\"listen\""), "{refused}");
    }

    #[test]
    fn every_receive_takes_from_the_listener_the_first_bound() {
        use event::wire::{Data, WireEvent};
        let hook = node("http://127.0.0.1:1/api/events", "key");
        let address = hook.inbound.bound(|| hook.bind()).expect("bound");
        let url = format!("http://{address}/hook");
        let grid = std::thread::spawn(move || {
            for round in 0..5u8 {
                let mut event = WireEvent::new(round.to_string(), "http://topic.local", "t");
                event.data = Some(Data::Binary(vec![round]));
                let delivery = webhook::deliver("/hook", &event).expect("written");
                webhook::push(&url, delivery, Some(Duration::from_secs(2))).expect("pushed");
            }
        });
        for round in 0..5u8 {
            let mut arrived = hook.receive().expect("received");
            assert_eq!(arrived.remove(0).taken().expect("taken").bytes, [round]);
        }
        grid.join().expect("grid");
    }

    #[test]
    fn a_batch_is_answered_once_every_event_has_its_verdict() {
        use event::wire::{Data, WireEvent};
        let hook = node("http://127.0.0.1:1/api/events", "key");
        let address = hook.inbound.bound(|| hook.bind()).expect("bound");
        let url = format!("http://{address}/hook");
        let grid = std::thread::spawn(move || {
            let events: Vec<WireEvent> = (0..2u8)
                .map(|n| {
                    let mut event = WireEvent::new(n.to_string(), "http://topic.local", "t");
                    event.data = Some(Data::Binary(vec![n]));
                    event
                })
                .collect();
            let timeout = Some(Duration::from_secs(2));
            let batch = || webhook::deliver_batch("/hook", &events).expect("written");
            let failed = webhook::push(&url, batch(), timeout).expect_err("failed");
            let accepted = webhook::push(&url, batch(), timeout).expect("delivered again");
            let refused = webhook::push(&url, batch(), timeout).expect_err("refused");
            (failed, accepted.status, refused)
        });
        let mut arrived = hook.receive().expect("received");
        assert_eq!(arrived.len(), 2);
        assert!(arrived.iter().all(Arrived::defers));
        let second = arrived.pop().expect("second");
        arrived.pop().expect("first").taken().expect("accepted");
        assert!(!grid.is_finished(), "no answer before the last verdict");
        second.failed().expect("failed");
        let again = hook.receive().expect("again");
        let taken: Vec<_> = again
            .into_iter()
            .map(|arrived| arrived.taken().expect("taken").bytes)
            .collect();
        assert_eq!(taken, [[0], [1]]);
        let mut third = hook.receive().expect("a third time");
        third
            .pop()
            .expect("second")
            .refused(transport::Refusal::Forbidden)
            .expect("refused");
        third.pop().expect("first").taken().expect("accepted");
        let (failed, accepted, refused) = grid.join().expect("grid");
        assert!(
            failed.retryable && failed.message.contains("503"),
            "{failed}"
        );
        assert_eq!(accepted, 200);
        assert!(
            !refused.retryable && refused.message.contains("400"),
            "not delivered again: {refused}"
        );
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
        assert_eq!(
            near.accept_one(&webhook).expect("handshake"),
            [] as [Taken; 0]
        );
        let arrived = near.accept_one(&webhook).expect("delivered");
        assert_eq!(arrived.len(), 1);
        assert_eq!(arrived[0].bytes, [0, 0xff, b'\r', b'\n']);
        assert!(arrived[0].origin_uri.starts_with(&format!("{topic}#")));
        let published = far_end.join().expect("thread");
        let origin = &arrived[0].origin_uri;
        assert_matches!(published, Event::Published(event) if envelope::origin(&event) == *origin);
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
    fn a_location_presents_the_topic_key_its_secret_holds() {
        use transport::secured::Kept;
        let drawn = Kept::drawn();
        let (listener, address) = socket::bind_tcp("127.0.0.1:0").expect("bind");
        let mut session = node("http://x/api/events", &drawn).session();
        let far_end = std::thread::spawn(move || session.serve_one(&listener).expect("served"));
        let topic = format!("http://{address}/api/events");
        let kept = std::sync::Arc::new(Kept::default());
        let near = EventGridTransport::open(&topic, Applies::Send, &[])
            .expect("built")
            .timing_out_after(Duration::from_secs(2))
            .secured_by(kept.secured("partner-x.grid"))
            .expect("Event Grid presents the topic key");
        let unset = near.send("", b"x").expect_err("not set yet");
        assert!(
            unset.retryable && unset.message.contains("is not set"),
            "{unset}"
        );
        kept.set("partner-x.grid", xcore::SecretKind::Password, &drawn);
        near.send("", b"keyed").expect("presented the secret");
        assert_matches!(
            far_end.join().expect("thread"),
            Event::Published(event) if envelope::data(&event) == b"keyed"
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

    /// The System Tests' edge payloads, written here so the crate does not
    /// depend on them, and one at the brim.
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

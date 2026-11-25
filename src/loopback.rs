//! The transport as its own far end (ADR-0051): the session stands as the
//! topic, takes the one publish, opens the handshake with the webhook this
//! transport listens as, and delivers to it, so what comes back went
//! through the topic and arrived the way a Receive Location takes it.

use std::net::TcpListener;

use net::Endpoint;
use transport::arrived::next_arrival;
use transport::error::{Result, protocol_error};
use transport::listening::Listening;
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback, both_ends, poke};
use transport::{ArrivalIdentity, Configured, Transport, socket};

use crate::{Event, EventGridTransport, ceiling};

/// What the loopback pair agrees on: one topic key.
const LOOPBACK_KEY: &str = "probe";

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

impl Loopback for EventGridTransport {
    fn arrival_identity(&self) -> ArrivalIdentity {
        ArrivalIdentity::Unnamed(
            "the service pushes it: the request says who the service is, not the publisher",
        )
    }

    fn ceiling(&self) -> Option<usize> {
        Some(ceiling())
    }

    /// A session listening for its one publish, bound at the topic URL's
    /// authority — `127.0.0.1:0` for the loopback — and the webhook it then
    /// validates and delivers to, bound where this transport listens: the
    /// far end is the topic and the subscription both, so what comes back
    /// went through the topic and arrived the way a Receive Location takes
    /// it.
    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        let transport = self.clone();
        let mut session = self.session();
        let (webhook, webhook_address) = self.bind()?;
        Ok(Box::new(Listening::new(
            move |listener: &TcpListener| {
                let published = match session.serve_one(listener)? {
                    Event::Published(event) => *event,
                    Event::Refused(code) => {
                        return Err(protocol_error(format!("the session refused: {code}")));
                    }
                };
                let url = format!("http://{webhook_address}/hook");
                let (delivered, taken) = both_ends(
                    move || {
                        let taken = transport.accept_one(&webhook)?;
                        if taken.is_empty() {
                            return transport.accept_one(&webhook);
                        }
                        Ok(taken)
                    },
                    || {
                        session
                            .validate(&url)
                            .and_then(|()| session.deliver(&url, &published))
                    },
                    || poke(&webhook_address),
                );
                delivered?;
                next_arrival(taken?, "delivered, but the webhook took nothing")
            },
            socket::bind_tcp(&Endpoint::parse(&self.topic_url)?.address())?,
        )))
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

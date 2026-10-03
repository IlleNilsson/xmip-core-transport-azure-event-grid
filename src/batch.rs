//! One answer shared by every event of one delivery.
//!
//! Event Grid delivers a batch in one `POST` and reads one status back for
//! all of it, while each event is a Stream of its own with its own
//! verdict. The answer is written once the last of them has its verdict
//! ([`status`]): `200` where every one was accepted; `503` where any
//! failed — Event Grid then delivers the whole batch again, and the events
//! already accepted arrive twice: at-least-once, never a loss; otherwise,
//! where any was refused, a status Event Grid does not retry for a webhook
//! (Azure Event Grid, *Delivery and retry*: `400`, `401` and `413` are not
//! retried), so the batch is not delivered again: its accepted events are
//! Xmip's and its refused ones are refused for good. Where an event's
//! acknowledgement is dropped without a verdict the answer is never
//! written; once the last is gone the reply goes too, which shuts the
//! connection, and Event Grid delivers again.

use http::inbound::Reply;
use http::server;
use net::http::Response;
use transport::together::together;
use transport::{Acknowledgement, Refusal, Verdict};

/// What Event Grid is answered where every event was accepted.
pub const ACCEPTED: u16 = 200;

/// What Event Grid is answered where an event was refused and no sender
/// went unidentified: `400 Bad Request`, which it does not retry.
pub const REFUSED: u16 = 400;

/// One acknowledgement for each of `events` events, all answering
/// `reply` once the last has its verdict (`transport::together`).
#[must_use]
pub fn acknowledgements(reply: Reply, events: usize) -> Vec<Acknowledgement> {
    together(
        events,
        |_, _| Ok(()),
        move |verdicts| {
            // One dropped without a verdict: the reply goes unanswered,
            // which shuts the connection.
            let Some(verdicts) = verdicts.iter().copied().collect::<Option<Vec<_>>>() else {
                return Ok(());
            };
            reply.answer(&Response::new(status(&verdicts)))
        },
    )
}

/// The one status a batch's verdicts earn: [`server::FAILED`] where any
/// failed, so Event Grid delivers it again; where none failed and any was
/// refused, `401` where a sender was not identified and [`REFUSED`]
/// otherwise — statuses Event Grid does not retry, where its `403` and
/// `422` it would; [`ACCEPTED`] where every one was.
#[must_use]
pub fn status(verdicts: &[Verdict]) -> u16 {
    if verdicts.contains(&Verdict::Failed) {
        return server::FAILED;
    }
    let unidentified = Verdict::Refused(Refusal::Unidentified);
    if verdicts.contains(&unidentified) {
        return server::status(unidentified);
    }
    if verdicts
        .iter()
        .any(|verdict| matches!(verdict, Verdict::Refused(_)))
    {
        return REFUSED;
    }
    ACCEPTED
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_outranks_refusal_which_outranks_acceptance() {
        let forbidden = Verdict::Refused(Refusal::Forbidden);
        let unknown = Verdict::Refused(Refusal::Unidentified);
        assert_eq!(status(&[Verdict::Accepted, Verdict::Accepted]), 200);
        assert_eq!(status(&[forbidden, Verdict::Failed]), 503);
        assert_eq!(status(&[Verdict::Accepted, forbidden]), 400);
        assert_eq!(
            status(&[Verdict::Refused(Refusal::Unacceptable), unknown]),
            401
        );
    }
}

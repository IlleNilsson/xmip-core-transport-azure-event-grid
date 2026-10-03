# xmip-core-transport-azure-event-grid

Azure Event Grid transport: a topic key over the REST API — publish a Stream as one wire event to a topic, receive as the webhook a subscription delivers to, answering the validation handshake — a topic endpoint is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## The WireEvent is the event capability's

The event a Stream travels in is
[xmip-core-event](https://github.com/IlleNilsson/xmip-core-event)'s
`wire::WireEvent` — its attributes, its JSON format, `data_base64`
for bytes — written and read there once (ADR-0065 clause 3), and a
published event's identity is minted by xmip-core's one generator. This
crate carried a second one of its own, with its own JSON writer and reader
and its own identifiers, until 2026-09-26. What stays here is Event Grid's
(`envelope`): the `se.xmip.stream` type a Stream is published under, the
one-mebibyte ceiling an event keeps and the share of it the envelope takes,
and a Stream's bytes out of an event another publisher wrote.

Requests go on connections kept between them (`http::endpoint::Connections`, offering HTTP/1.1): the transport holds them and hands them to every client it makes, so a call costs one exchange and not a connect, a TLS handshake and a `Connection: close`, as it did until 2026-09-27.

A Receive Location keeps its listener, bound on the first receive, and the connections senders keep open on it (`http::inbound::Inbound`): each receive takes the next request from whichever sends first, where until 2026-09-27 each receive bound a listener of its own, answered one request with `Connection: close`, and refused a request that came between two receives.

## Acknowledged after the receive cycle

Event Grid waits on its connection for the answer to a delivery until the runtime's whole receive cycle has ended for every event in it (runtime-model section 5): `200` where every event was accepted; `503` where any failed, and Event Grid delivers the whole batch again by its retry policy — events of it already accepted arrive twice, at-least-once and never a loss; otherwise, where any was refused, `401` (a sender not identified) or `400`, the statuses Event Grid does not retry for a webhook (its `403` and `422` it would), so the batch is not delivered again: its accepted events are Xmip's and its refused ones refused for good (`batch::status`). The validation handshake and the subscription validation are answered at once. No round trip is added: the answer is the one Event Grid always waited for, only later.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.

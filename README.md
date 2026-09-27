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

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.

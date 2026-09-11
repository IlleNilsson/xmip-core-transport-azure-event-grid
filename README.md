# xmip-core-transport-azure-event-grid

Azure Event Grid transport: a topic key over the REST API — publish a Stream as one CloudEvent to a topic, receive as the webhook a subscription delivers to, answering the validation handshake — a topic endpoint is a Location. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.

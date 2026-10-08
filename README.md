This repository contains HTTP/2 parser used by the ParsePlz organisation.

## Header bytes and sensitivity

Ordinary header values are byte-preserving, including valid non-UTF-8 values.
Malformed header names and values are rejected after the complete compression
block is decoded so the connection's compression table remains synchronized.

Mark an HTTP/2 `header_plz::Header` with `set_sensitive(true)` to require
never-indexed HPACK encoding. Request pseudoheaders use
`RequestLine::set_sensitive(RequestPseudoHeader::Path, true)` (and the other
selectors); response status uses `ResponseLine::set_sensitive(true)`.
Construct the message with its marked info line using `Message::new`.
Incoming never-indexed flags are preserved through request/response forwarding.
Use metadata-preserving decomposition methods when reconstructing headers or
info lines; the legacy `into_inner`/`into_parts` methods discard those flags.
Sensitivity changes compression policy, not semantic equality.

This checkout pins the metadata-aware `header-plz` source to commit
`67d43e6e912272e12561db5bfa40f6067bb9bd78` in `caido/primitives`
(branch `feat/hpack-sensitivity-metadata`). The crates.io patch keeps transitive
`http-plz` users on the same header type. Fetching the private Git source requires
GitHub SSH access. Root patches are not inherited by downstream consumers;
registry publication still requires coordinated dependency releases.

## Server push

Servers can call `SendResponse::push_request(request)` before finishing the
associated client-initiated stream. It returns a separate `SendResponse` for the
promised response, supporting both buffered and streaming responses. Continue
polling the server connection to send the promise and response.

Promised requests must use GET or HEAD, include an authority, and contain no
body, trailers, or protocol extension. HEAD responses must be headers-only.
Push honors the client's `SETTINGS_ENABLE_PUSH` (enabled when omitted) and its
concurrent-stream limit. SPA connections do not support push.

Clients reject push by default (`SETTINGS_ENABLE_PUSH=0`). Opt in with
`ClientBuilder::enable_push(true)`, then use `ClientConnection::push().await`
(or `poll_push`) to receive `Option<Result<(Request, StreamingResponseFuture),
OpError>>`. The request is delivered as soon as its promise arrives, independently
of the parent request. Awaiting the response future yields response headers and a
`RecvBody`; consume `BodyFrame::Data` and `BodyFrame::Trailers` incrementally or
accumulate them for a buffered response. Dropping the response future or an
unfinished body cancels that pushed stream.

`push` drives the transport. Continue driving the connection while awaiting
responses or bodies, but do not concurrently poll `push` and the connection's
`Future`: they share a transport waker. Clean closure yields `None`; terminal
connection errors yield `Some(Err(...))`. Reservations do not count toward the
active-stream limit until response headers arrive. An independent limit of
1,024 queued pushes plus still-reserved streams handed to callers protects
against unbounded reservations and acceptance queues; exceeding it closes the
connection with `ENHANCE_YOUR_CALM`.

Before consuming or caching a promised response, callers must verify that the
server is authoritative for the promised request's origin, or is an authorized
proxy. This generic IO API cannot inspect TLS certificates to perform that
check. Servers never advertise the client-only `SETTINGS_ENABLE_PUSH` setting;
clients reject a server that sends it.

## HTTP/2 conformance tests

Run the non-conformance client and server integration suites:

```sh
mise run test
```

The upstream `h2spec` 2.1.1 conformance suite runs in GitHub Actions on Linux.
The CI workflow uses `mise run test:conformance` to build
`examples/h2spec_server.rs`, listen on `127.0.0.1:5928`, and write a JUnit
report to `artifacts/h2spec.junit.xml`. Conformance tests are not part of the
local test workflow; no Docker setup is required.


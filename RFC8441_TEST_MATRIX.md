# Extended CONNECT test coverage

This library implements HTTP/2 transport, not a WebSocket frame codec. The
requirements below are grounded in [RFC 8441](https://www.rfc-editor.org/rfc/rfc8441).
WebSocket frame validation and application handshake policy belong to callers.

| Requirement | Existing coverage before this audit | Added coverage |
| --- | --- | --- |
| §3 / §9.1: setting 0x8 defaults to disabled; values must be 0 or 1 | `client_request`: default disabled, enabling initial settings, invalid value 2; `server_response`: disabled/enabled advertisement | Explicit zero, later enabling, repeated one, omitted setting, invalid downgrade across frames and within one frame |
| §3: client may use extended CONNECT only after receiving value 1 | Capability query tested, but outbound requests were not gated | Both buffered and streaming requests reject unnegotiated extended CONNECT without consuming a stream ID; valid requests work after negotiation |
| §3: receipt of setting by a server has no impact | No explicit assertion | Client advertisement does not enable inbound extended CONNECT on a disabled server |
| §4: `:protocol` is single-valued and names an HTTP upgrade token | Generic pseudo-header duplicate/order machinery; protocol on GET rejected | Token syntax (not registry membership), raw duplicate and ordering validation with HPACK state recovery, request-only pseudo-headers rejected on responses |
| §4: extended requests retain `:scheme` and `:path`; §5 WebSocket target uses authority | Serializer unit tests; missing scheme/path tests also omitted authority | Isolated missing/empty fields and complete URI/protocol/header reconstruction; existing malformed tests now include authority |
| §5: HTTP/2 forbids Connection and Upgrade; other handshake headers pass through | Generic forbidden-header tests | Extended CONNECT validation and header preservation; no Sec-WebSocket-Key/Accept processing introduced |
| §5: successful opening handshake exposes a bidirectional stream | Enabled handshake test and tunnel integration tests | Response status/content-length matrix for classic and extended CONNECT, buffered and streaming APIs |
| §5: END_STREAM corresponds to orderly closure; CANCEL reset corresponds to abnormal closure | Tunnel half-close, reset, drop, small-read tests; gated transport flush/shutdown tests | Existing coverage retained |
| §1 / §6: tunnels share HTTP/2 cancellation and multiplexing behavior | Single tunnel tests and generic stream tests | Reset one tunnel while a sibling tunnel and ordinary HTTP request continue |

## Adjacent HTTP/2 / HTTP semantics

Successful CONNECT responses ignore Content-Length, while rejected CONNECT
responses retain normal body validation. These rules are shared with classic
CONNECT; the response matrix checks 200, 201, 299, 300, 403 and malformed length
syntax on successful extended CONNECT.

The ordinary `h2spec` task tests core HTTP/2 and HPACK, not RFC 8441. Run the Rust
workspace tests for the extended CONNECT regressions, and run
`mise run test:conformance-docker` for the separate core conformance suite.

## Validation boundaries

`:protocol` values are checked as nonempty ASCII HTTP tokens, not against a
hard-coded IANA registry allowlist; protocol support remains an application
decision. The request API requires a complete target (scheme, nonempty authority,
and path) for all extended CONNECT protocols. RFC 8441 §4 explicitly adds
scheme/path requirements, and §5 describes authority for WebSocket targets;
requiring authority for other extended protocols is this API's validation policy.
Ordinary authorityless requests remain supported.

## Caller responsibilities

- Map `ws`/`wss` URLs to HTTP/HTTPS targets and choose the `websocket` protocol.
- Validate Origin, WebSocket version, subprotocol and extension negotiation.
- Check response success before constructing/using a tunnel as a WebSocket.
- Implement WebSocket framing, masking, fragmentation, ping/pong and closing.
- Route extended CONNECT to an appropriate service, not an automatic TCP proxy
  to the request authority. This library exposes the request; it opens no such
  outbound connection.

No test here claims WebSocket framing compliance, TLS policy compliance, browser
interoperability, or exhaustive coverage of all possible frame interleavings.

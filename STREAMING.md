# Streaming request and response bodies

The buffered `send_request`, `ResponseFuture`, `accept`, and `send_response`
APIs remain available. Streaming is opt-in:

- Client: `send_request_streaming(request_head, end_stream)` returns a
  `StreamingResponseFuture` and `SendBody`. The future resolves to a response
  head and `RecvBody` as soon as final headers arrive.
- Server: `accept_streaming` (or `poll_accept_streaming`) returns a request
  head, `RecvBody`, and `SendResponse`. Call `send_response_streaming` to
  obtain a response `SendBody`.
- `SendBody::send_data(Bytes, end_stream)` waits for bounded queue space.
  `send_trailers(HeaderMap)` terminates the body with trailers. Sending an
  empty DATA chunk with `end_stream = true` also terminates it.
- `RecvBody::frame()` yields `BodyFrame::Data` or `BodyFrame::Trailers`, then
  `None`. With the `stream` feature it also implements `futures_core::Stream`.

Streaming heads must not already contain a body or trailers. The
`end_stream` argument indicates whether headers alone complete the body.
Streaming is incompatible with single-packet-attack mode.

## Driving the connection

Client connections must continue being polled (usually in a spawned task).
On the server, keep polling `poll_closed` or accepting requests while body
handles are in use. Awaiting a body without driving its connection cannot
make progress.

Select streaming acceptance before the server's first connection poll;
do not mix buffered and streaming acceptance on one server connection.
Buffered acceptance intentionally waits for complete requests and preserves
its existing ordering behavior; streaming acceptance delivers headers early.

## Backpressure and cancellation

Receive window credit is held for queued streaming DATA and returned when
`frame()` yields the chunk. Process a chunk before asking for another;
collecting chunks into your own unbounded queue defeats backpressure.
The existing receive buffer limit remains a safety limit, so configure it
at least as large as the advertised receive window.

Send completion means bytes have been queued, not necessarily flushed to
the socket. Send admission is bounded per stream; HTTP/2 connection and
stream windows control draining. A large chunk is admitted incrementally.
Canceling `send_data` may leave a prefix queued. For cancellation-safe
resumption, use `poll_send_data` with an owned `Bytes`: it advances that
buffer to the remaining suffix on every poll.

Dropping an unfinished body cancels the stream. Consume through `None`
(or finish sending DATA/trailers) to complete normally. Errors are yielded
once by `RecvBody`, followed by `None`.

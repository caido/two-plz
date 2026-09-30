This repository contains HTTP/2 parser used by the ParsePlz organisation.

## HTTP/2 conformance tests

Run the non-conformance client and server integration suites:

```sh
mise run test
```

Run the upstream `h2spec` 2.1.1 suite against the clear-text server:

```sh
mise run test:conformance
```

Run the same conformance suite in a Linux x86_64 Docker container:

```sh
mise run test:conformance-docker
```

The conformance task builds `examples/h2spec_server.rs`, listens on
`127.0.0.1:5928`, and writes a JUnit report to
`artifacts/h2spec.junit.xml`. The GitHub Actions workflow uploads that report.
Docker is required only for `mise run test:conformance-docker`.


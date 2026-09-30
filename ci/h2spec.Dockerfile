FROM --platform=linux/amd64 rust:1-bookworm

WORKDIR /workspace
COPY . .

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl --fail --location https://mise.run | sh \
    && chmod +x .mise/tasks/*

ENV PATH="/root/.local/bin:${PATH}"
CMD ["mise", "run", "test:conformance"]

# The `pipelined` binary for the compose stack. Build context is the repository root:
#
#   docker compose -f deploy/compose.yaml build pipeline
#
# The regex crate compiles the bundled PCRE2 from C, hence the full Rust image (it carries a
# C toolchain) rather than the slim one.
FROM rust:1.96-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p fusion-pipeline --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/pipelined /usr/local/bin/pipelined
ENTRYPOINT ["pipelined"]
CMD ["--config", "/etc/pipeline/pipeline.yaml"]
